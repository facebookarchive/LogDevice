/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>

#include <folly/io/async/SSLContext.h>
#include <folly/portability/OpenSSL.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * @file Loads the SSL context from the specified files, reloads it if it gets
 *       older than the defined expiration interval, provides a shared_ptr to
 *       folly::SSLContext. Does not implement any thread safety mechanics.
 */

class SSLFetcher {
 public:
  static const char* IDENTITY_TYPE_OID;
  static const char* BASIC_CONSTRAINTS_OID;

  SSLFetcher(const std::string& cert_path,
             const std::string& key_path,
             const std::string& ca_path,
             std::chrono::seconds refresh_interval)
      : cert_path_(cert_path),
        key_path_(key_path),
        ca_path_(ca_path),
        refresh_interval_(refresh_interval) {}

  /**
   * @param loadCert          Defines whether or not the certificate will be
   *                          loaded into the SSLContext.
   * @param ssl_accepting     Defines whether the SSLContext is for accepting or
   *                          connecting side of the socket.
   * @param null_ciphers_only This parameter is only used if ssl_accepting is
   *                          false. If set to true the ciphers are limited to
   *                          eNULL ciphers.
   *
   * @return                  a pointer to the created SSLContext or a null
   *                          pointer if the certificate could not be loaded.
   */
  std::shared_ptr<folly::SSLContext> getSSLContext(bool loadCert,
                                                   bool ssl_accepting,
                                                   bool null_ciphers_only) {
    if (!context_ ||
        requireContextUpdate(loadCert, ssl_accepting, null_ciphers_only)) {
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
        // list of valid ciphers. It is up to the connecting socket to limit
        // the list of valid ciphers to enable or disable encryption.
        std::string null_ciphers = "eNULL";
#if FOLLY_OPENSSL_IS_110
        null_ciphers += ":@SECLEVEL=0";
#endif
        if (ssl_accepting) {
          context_->ciphers("ALL:!COMPLEMENTOFDEFAULT:" + null_ciphers +
                            ":@STRENGTH");
        } else if (null_ciphers_only) {
          ld_info("Creating SSL context using eNULL ciphers");
          context_->ciphers(null_ciphers);
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
        // TODO: remove callback before open-sourcing
        SSL_CTX_set_verify(
            context_->getSSLCtx(), SSL_VERIFY_PEER, verify_callback);

        // Disabling sessions caching
        SSL_CTX_set_session_cache_mode(
            context_->getSSLCtx(), SSL_SESS_CACHE_OFF);

      } catch (const std::exception& ex) {
        ld_error("Failed to load SSL certificate, ex: %s", ex.what());
        context_.reset();
        return nullptr;
      }
    }
    return context_;
  }

 private:
  const std::string cert_path_;
  const std::string key_path_;
  const std::string ca_path_;
  const std::chrono::seconds refresh_interval_;

  std::shared_ptr<folly::SSLContext> context_;
  std::chrono::time_point<std::chrono::steady_clock> last_loaded_;
  bool last_null_cipher_only_ = false;
  bool last_accepting_state_ = false;
  bool last_load_cert_ = false;

  // verification callback for ssl context. Used to check extra critical
  // extensions of a certificate.
  static int verify_callback(int preverify_ok, X509_STORE_CTX* x509_ctx);

  // a context update is required when refresh_interval_ has passed or when any
  // of the input information is changed
  bool requireContextUpdate(bool loadCert,
                            bool ssl_accepting,
                            bool null_ciphers_only) {
    auto now = std::chrono::steady_clock::now();
    bool update = now - last_loaded_ > refresh_interval_ ||
        loadCert != last_load_cert_ || ssl_accepting != last_accepting_state_ ||
        null_ciphers_only != last_null_cipher_only_;

    if (update) {
      last_loaded_ = now;
      last_null_cipher_only_ = null_ciphers_only;
      last_accepting_state_ = ssl_accepting;
      last_load_cert_ = loadCert;
    }
    return update;
  }
};

}} // namespace facebook::logdevice
