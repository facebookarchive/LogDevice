/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SSLFetcher.h"

#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

std::shared_ptr<folly::SSLContext>
SSLFetcher::getSSLContext(bool loadCert, bool ssl_accepting) {
  if (!context_ || requireContextUpdate(loadCert, ssl_accepting)) {
    try {
      context_.reset(new folly::SSLContext());
      context_->loadTrustedCertificates(ca_path_.c_str());
      context_->loadClientCAList(ca_path_.c_str());

      if (loadCert) {
        context_->loadCertificate(cert_path_.c_str());
        context_->loadPrivateKey(key_path_.c_str());
      }

      // The node that accepts the connection must present all valid ciphers
      // that the connecting socket can use. Since we want to separate
      // encryption and authentication, we include eNULL ciphers in the
      // list of valid ciphers [DEPRECATED]. It is up to the connecting socket
      // to limit the list of valid ciphers to enable or disable encryption.
      std::string null_ciphers = "eNULL";
#if FOLLY_OPENSSL_IS_110
      null_ciphers += ":@SECLEVEL=0";
#endif
      if (ssl_accepting) {
        context_->ciphers("ALL:!COMPLEMENTOFDEFAULT:" + null_ciphers +
                          ":@STRENGTH");
      } else {
        context_->ciphers("ALL:!COMPLEMENTOFDEFAULT:!eNULL:@STRENGTH");
      }

      // Dropping the buffers we are not using and not compressing data
      context_->setOptions(SSL_OP_NO_COMPRESSION);
      SSL_CTX_set_mode(context_->getSSLCtx(), SSL_MODE_RELEASE_BUFFERS);

      // Check peers cert not their hostname
      context_->authenticate(true, false);

      // Don't force the client to use a certificate
      context_->setVerificationOption(folly::SSLContext::VERIFY);

      // Don't force client to use a certificate, however still verify
      // server certificate. If client does provide a certificate, then it is
      // also verifed by the server.
      SSL_CTX_set_verify(context_->getSSLCtx(), SSL_VERIFY_PEER, nullptr);

      // Disabling sessions caching
      SSL_CTX_set_session_cache_mode(context_->getSSLCtx(), SSL_SESS_CACHE_OFF);

      // keep track of context creation parameters
      updateState(loadCert,
                  ssl_accepting,
                  SSL_CTX_get0_certificate(context_->getSSLCtx()));
    } catch (const std::exception& ex) {
      ld_error("Failed to load SSL certificate, ex: %s", ex.what());
      context_.reset();
      return nullptr;
    }
  }
  return context_;
}

std::shared_ptr<const fizz::server::FizzServerContext>
SSLFetcher::getFizzServerContext() {
  if (!fizz_srv_context_ ||
      requireContextUpdate(true /* loadCert */, true /* ssl_accepting */)) {
    try {
      auto context = std::make_shared<fizz::server::FizzServerContext>();
      // request client certificate, but don't force to use it
      auto verifier = createCertVerifier<fizz::DefaultCertificateVerifier>(
          fizz::VerificationContext::Server);
      context->setClientCertVerifier(std::move(verifier));
      auto cert_mgr = std::make_unique<fizz::server::CertManager>();
      auto cert = createSelfCert();
      auto x509 = cert->getX509();
      cert_mgr->addCert(std::move(cert), true);
      context->setCertManager(std::move(cert_mgr));
      context->setVersionFallbackEnabled(true);
      context->setClientAuthMode(fizz::server::ClientAuthMode::Optional);

      fizz_srv_context_ = std::move(context);
      // keep track of context creation parameters
      updateState(true, true, x509.get());

    } catch (const std::exception& ex) {
      ld_error("Failed to create SSL context, ex: %s", ex.what());
      fizz_srv_context_.reset();
    }
  }
  return fizz_srv_context_;
}

std::pair<std::shared_ptr<const fizz::client::FizzClientContext>,
          std::shared_ptr<const fizz::CertificateVerifier>>
SSLFetcher::getFizzClientContext(bool loadCert) {
  if (!fizz_cli_context_ ||
      requireContextUpdate(loadCert, false /* ssl_accepting */)) {
    try {
      auto context = std::make_shared<fizz::client::FizzClientContext>();
      fizz_cli_verifier_ = createCertVerifier<fizz::DefaultCertificateVerifier>(
          fizz::VerificationContext::Client);
      folly::ssl::X509UniquePtr x509{nullptr};
      if (loadCert) {
        auto cert = createSelfCert();
        x509 = cert->getX509();
        context->setClientCertificate(std::move(cert));
      }
      fizz_cli_context_ = std::move(context);
      // keep track of context creation parameters
      updateState(loadCert, false, x509.get());

    } catch (const std::exception& ex) {
      ld_error("Failed to create client SSL context, ex: %s", ex.what());
      fizz_cli_context_.reset();
      fizz_cli_verifier_.reset();
    }
  }
  return {fizz_cli_context_, fizz_cli_verifier_};
}

template <class CertVerifierT>
std::shared_ptr<const CertVerifierT>
SSLFetcher::createCertVerifier(fizz::VerificationContext verCtx) const {
  folly::ssl::X509StoreUniquePtr store(X509_STORE_new());
  if (!store) {
    throw std::runtime_error("Failed to create X509 store.");
  }
  if (X509_STORE_load_locations(
          store.get(), ca_path_.c_str(), nullptr /* directory */) == 0) {
    throw std::runtime_error(folly::SSLContext::getErrors());
  }
  return std::make_shared<const CertVerifierT>(verCtx, std::move(store));
}

std::unique_ptr<fizz::SelfCert> SSLFetcher::createSelfCert() const {
  std::string cert_data;
  if (!folly::readFile(cert_path_.c_str(), cert_data)) {
    throw std::runtime_error("failed to load certificates");
  }

  if (cert_path_ == key_path_) {
    return fizz::CertUtils::makeSelfCert(cert_data, cert_data);
  }

  std::string key_data;
  if (!folly::readFile(key_path_.c_str(), key_data)) {
    throw std::runtime_error("failed to load certificates");
  }
  return fizz::CertUtils::makeSelfCert(cert_data, key_data);
}

bool SSLFetcher::requireContextUpdate(bool loadCert, bool ssl_accepting) const {
  auto now = std::chrono::steady_clock::now();
  bool update = (last_load_cert_ && now - last_loaded_ > refresh_interval_) ||
      loadCert != last_load_cert_ || ssl_accepting != last_accepting_state_;
  return update;
}

void SSLFetcher::updateState(bool loadCert, bool ssl_accepting, X509* cert) {
  ld_check(!loadCert || cert);
  last_load_cert_ = loadCert;
  last_accepting_state_ = ssl_accepting;
  if (stats_) {
    STAT_INCR(stats_, ssl_context_created);
  }
  if (!cert) {
    return;
  }
  last_loaded_ = std::chrono::steady_clock::now();
}

}} // namespace facebook::logdevice
