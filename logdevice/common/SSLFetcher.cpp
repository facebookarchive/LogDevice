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

SSLFetcher::SSLFetcher(const std::string& cert_path,
                       const std::string& key_path,
                       const std::string& ca_path,
                       bool load_certs,
                       StatsHolder* stats)
    : cert_path_(cert_path),
      key_path_(key_path),
      ca_path_(ca_path),
      load_certs_(load_certs),
      stats_(stats) {
  reloadSSLContext();
}

std::shared_ptr<folly::SSLContext> SSLFetcher::getSSLContext() const {
  return context_;
}

void SSLFetcher::reloadSSLContext() {
  try {
    context_.reset(new folly::SSLContext());
    context_->loadTrustedCertificates(ca_path_.c_str());
    context_->loadClientCAList(ca_path_.c_str());

    if (load_certs_) {
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

    if (stats_) {
      STAT_INCR(stats_, ssl_context_created);
    }
  } catch (const std::exception& ex) {
    ld_error("Failed to load SSL certificate, ex: %s", ex.what());
    context_.reset();
  }
}

}} // namespace facebook::logdevice
