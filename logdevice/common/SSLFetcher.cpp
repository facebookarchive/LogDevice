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

std::shared_ptr<folly::SSLContext> SSLFetcher::getSSLContext(bool loadCert) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (requireContextUpdate(loadCert)) {
    try {
      context_.reset(new folly::SSLContext());
      context_->loadTrustedCertificates(ca_path_.c_str());
      context_->loadClientCAList(ca_path_.c_str());

      if (loadCert) {
        context_->loadCertificate(cert_path_.c_str());
        context_->loadPrivateKey(key_path_.c_str());
      }

      context_->ciphers("ALL:!COMPLEMENTOFDEFAULT:!eNULL:@STRENGTH");

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
      updateState(loadCert, SSL_CTX_get0_certificate(context_->getSSLCtx()));
    } catch (const std::exception& ex) {
      ld_error("Failed to load SSL certificate, ex: %s", ex.what());
      context_.reset();
      return nullptr;
    }
  }
  return context_;
}

bool SSLFetcher::requireContextUpdate(bool loadCert) const {
  auto now = std::chrono::steady_clock::now();
  bool update = !state_.context_created_ ||
      (state_.last_load_cert_ &&
       now - state_.last_loaded_ > refresh_interval_) ||
      loadCert != state_.last_load_cert_;
  return update;
}

void SSLFetcher::updateState(bool loadCert, X509* cert) {
  ld_check(!loadCert || cert);
  ld_check(requireContextUpdate(loadCert));
  state_.last_load_cert_ = loadCert;
  state_.context_created_ = true;
  if (stats_) {
    STAT_INCR(stats_, ssl_context_created);
  }
  if (!cert) {
    return;
  }
  state_.last_loaded_ = std::chrono::steady_clock::now();
}

}} // namespace facebook::logdevice
