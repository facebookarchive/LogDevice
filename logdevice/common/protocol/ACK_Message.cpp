/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/ACK_Message.h"

#include <openssl/ssl.h>

#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

/**
 * Check validity of an ACK received.
 *
 * @param hdr  Header of the ACK message.
 * @param from Address of the sender.
 * @return Message::Disposition::ERROR if there is an error, or
 *         Message::Disposition::NORMAL otherwise.
 *         If there is an error, err is set to:
 *         - E::PROTO if we got an ACK from the active side of the connection;
 *         - E::BADMSG if the content of the message is invalid.
 */
static Message::Disposition checkValidity(const ACK_Header& hdr,
                                          const Address& from) {
  if (from.isClientAddress()) {
    ld_error("PROTOCOL ERROR: got an ACK from the active side of "
             "connection %s.",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  if (hdr.status != E::OK) {
    if (hdr.status == E::PROTONOSUPPORT ||
        hdr.status == E::DESTINATION_MISMATCH || hdr.status == E::ACCESS ||
        hdr.status == E::INVALID_CLUSTER || hdr.status == E::INTERNAL) {
      ld_warning("Server %s rejected our connection. Reason: %s",
                 Sender::describeConnection(from).c_str(),
                 error_description(hdr.status));
      err = hdr.status;
    } else {
      ld_error("BAD MESSAGE: invalid status code %u in an ACK we got "
               "from %s. Expected OK, PROTONOSUPPORT, INVALID_CLUSTER, "
               "DESTINATION_MISMATCH, or ACCESS.",
               (unsigned)hdr.status,
               Sender::describeConnection(from).c_str());
      err = E::BADMSG;
    }
    return Message::Disposition::ERROR;
  }

  if (!ClientID::valid(hdr.client_idx)) {
    ld_error("BAD MESSAGE: got an ACK with an invalid client id: %d",
             hdr.client_idx);
    err = E::BADMSG;
    return Message::Disposition::ERROR;
  }

  Worker* w = Worker::onThisThread();
  auto cluster_node_identity =
      w->processor_->security_info_->getClusterNodeIdentity();
  auto principal_parser = w->processor_->security_info_->getPrincipalParser();

  // If the authentication type is set to SSL and a cluster node identity is
  // configured, we verify that the presented certificate contains the required
  // identity.
  if (principal_parser && !cluster_node_identity.empty() &&
      principal_parser->getAuthenticationType() == AuthenticationType::SSL) {
    std::string idType, identity;
    if (folly::split(':', cluster_node_identity, idType, identity)) {
      X509* cert = w->sender().getPeerCert(from);
      if (cert) {
        // We only support server authentication for SSL connections. If the
        // server presents a certificate, we verify that the bundled identity
        // matches what's configured for cluster nodes. On the other hand,
        // if a certificate is not associated with the peer, we can assume that
        // this is because the connection is not SSL (but rather cleartext),
        // otherwise the SSL handshake would have failed already at this point
        // since we set SSL_VERIFY_PEER option in the SSL context.
        // Note we use 1 as size as it is ignored anyway for SSL certficate
        PrincipalIdentity principal = principal_parser->getPrincipal(cert, 1);
        X509_free(cert);
        if (!principal.match(idType, identity)) {
          RATELIMIT_ERROR(std::chrono::seconds(1),
                          1,
                          "Untrusted cluster node identity (%s), expecting %s. "
                          "Rejecting with E::ACCESS.",
                          principal.toString().c_str(),
                          cluster_node_identity.c_str());
          err = E::ACCESS;
          return Message::Disposition::ERROR;
        }
      }
    }
  }

  return Message::Disposition::NORMAL;
}

template <>
Message::Disposition ACK_Message::onReceived(const Address& from) {
  Message::Disposition disp = checkValidity(header_, from);

  if (disp == Message::Disposition::NORMAL) {
    // When status is E::OK, the server should set proto to a value in the range
    // that was provided provided in the HELLO message we sent.
    if (header_.proto < Compatibility::MIN_PROTOCOL_SUPPORTED ||
        header_.proto > Worker::settings().max_protocol) {
      ld_error("BAD MESSAGE: got an ACK with unexpected protocol version: %u. "
               "Expected a version in range [%hu, %hu].",
               header_.proto,
               Compatibility::MIN_PROTOCOL_SUPPORTED,
               Worker::settings().max_protocol);
      err = E::BADMSG;
      return Disposition::ERROR;
    }

    ld_debug("Server %s granted access and assigned us id %s (protocol version "
             "%u)",
             Sender::describeConnection(from).c_str(),
             ClientID(header_.client_idx).toString().c_str(),
             header_.proto);
  }

  return disp;
}

}} // namespace facebook::logdevice
