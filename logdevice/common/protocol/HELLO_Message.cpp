/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/HELLO_Message.h"

#include <memory>

#include <folly/Optional.h>
#include <folly/json.h>
#include <openssl/ssl.h>

#include "logdevice/common/ClientHelloInfoTracer.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/protocol/ACK_Message.h"
#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

// solves a linking issue with std::min
const size_t HELLO_Header::CREDS_SIZE_V1;

using Node = facebook::logdevice::configuration::Node;
/**
 * @param min   Min protocol version the client supports.
 * @param max   Max protocol version the client supports.
 * @param proto The protocol that should be used to talk to the client is
 *              written here if this function returns 0.
 *
 * @return 0 If we can talk to the client and the protocol to be used is written
 *         to proto. Returns -1 if we don't support the client's protocol and
 *         err is set to E::PROTONOSUPPORT.
 */
static int checkProto(uint16_t min, uint16_t max, uint16_t* proto) {
  uint16_t max_protocol = Worker::settings().max_protocol;
  ld_check(max_protocol >= Compatibility::MIN_PROTOCOL_SUPPORTED);
  ld_check(max_protocol <= Compatibility::MAX_PROTOCOL_SUPPORTED);

  if (Compatibility::MIN_PROTOCOL_SUPPORTED > max) {
    // We cannot talk to the client because it is too old.
    err = E::PROTONOSUPPORT;
    return -1;
  }

  if (min > max_protocol) {
    // The client won't be able to talk to us because we are too old.
    err = E::PROTONOSUPPORT;
    return -1;
  }

  *proto = std::min(max_protocol, max);
  ld_check(*proto >= Compatibility::MIN_PROTOCOL_SUPPORTED);
  ld_check(*proto <= max_protocol);
  return 0;
}

/**
 * @param peer_nid  The nodeID of the peer.
 * @param from      Address of the sender.
 *
 * @return          returns true if the IP address of the sender is the same
 *                  as the IP address of peer_nid in the config file. Returns
 *                  false otherwise.
 */
static bool isValidServerConnection(const NodeID& peer_nid,
                                    const Address& from) {
  assert(peer_nid.isNodeID());

  const auto& nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  const auto& peer_svc =
      nodes_configuration->getNodeServiceDiscovery(peer_nid.index());

  // Could not find a corresponding node in the config file
  if (peer_svc == nullptr) {
    ld_info("Got a HELLO from %s claiming to be a server with nodeID that is "
            "not in the config file.",
            Sender::describeConnection(from).c_str());
    return false;
  }

  const Sockaddr& conn_addr =
      Worker::onThisThread()->sender().getSockaddr(from);
  if (conn_addr == Sockaddr::INVALID) {
    ld_critical("INTERNAL ERROR: could not find socket address of "
                "HELLO_Message from %s",
                Sender::describeConnection(from).c_str());
    // This should never happen as we are calling getSockAddr of the
    // calling socket object.
    ld_check(false);
    return false;
  }

  // Only do IP authentication for real IP addresses
  if (!peer_svc->address.isUnixAddress() && !conn_addr.isUnixAddress()) {
    // If the IP addresses of the sender matches the IP address of the
    // peer_node_id_ in the socket, then we can be reasonably assured that
    // the connection is from that server node.
    if (peer_svc->address.getAddress() == conn_addr.getAddress() ||
        (peer_svc->ssl_address &&
         peer_svc->ssl_address->getAddress() == conn_addr.getAddress())) {
      return true;
    }
  }

  return false;
}

/**
 * Gets the authentication data for the connection and verifies that the
 * connection is allowed. If the connection is permitted a principal will
 * be assigned to the calling Socket Object. If it is not, the status
 * in the ackhdr will be set to E::ACCESS or E::INTERNAL.
 *
 * @param hellohdr      Header of the HELLO/HELLO message.
 * @param ackhdr        Header of the ACK message to send as a response.
 *                      Set the status of that header to E::ACCESS if the
 *                      credentials are invalid.
 * @param from          Address of the sender.
 */
template <typename HelloHeader, typename AckHeader>
static PrincipalIdentity checkAuthenticationData(const HelloHeader& hellohdr,
                                                 AckHeader& ackhdr,
                                                 const Address& from,
                                                 const std::string& csid) {
  Worker* w = Worker::onThisThread();

  auto principal_parser = w->processor_->security_info_->getPrincipalParser();

  PrincipalIdentity principal;

  // Some fields of PrincipalIdentity are populated by PrincipalParser,
  // others - by this function here. Call it after assigning to `principal`, but
  // before returning or using it.
  // This seems hacky; PrincipalParser should populate a provided
  // PrincipalIdentity instead of creating and returning a new one?
  auto fill_out_client_info_in_principal = [&] {
    principal.csid = csid;
    Sockaddr client_sock_addr = Sender::sockaddrOrInvalid(from);
    std::string client_sock_str = client_sock_addr.valid()
        ? client_sock_addr.toStringNoPort()
        : std::string("UNKNOWN");
    principal.client_address = client_sock_str;
  };

  if (principal_parser != nullptr) {
    bool useAuthenticationData = true;

    // If enable_server_ip_authentication is set, we ignore the credentials
    // that were provided by servers. Connections are identified as server
    // nodes if the peer_node_id_ is set in the calling socket object, and
    // the IP address of the connection matches the IP address of that node
    // in the configuration file.
    if (Worker::getConfig()->serverConfig()->authenticateServersByIP()) {
      NodeID peer_nid = w->sender().getNodeID(from);
      if (peer_nid.isNodeID()) {
        if (isValidServerConnection(peer_nid, from)) {
          principal = PrincipalIdentity(Principal::CLUSTER_NODE);
          useAuthenticationData = false;
        } else {
          // peer provided a node_id but the connection is not from a
          // valid nodeID
          ackhdr.status = E::ACCESS;
          fill_out_client_info_in_principal();
          return principal;
        }
      }
    }
    // We have not authenticated by IP, use provided authentication data
    if (useAuthenticationData) {
      // obtain principal from authentication data
      switch (principal_parser->getAuthenticationType()) {
        case AuthenticationType::SELF_IDENTIFICATION: {
          // authentication data are already stored in the hellohdr.
          principal = principal_parser->getPrincipal(
              hellohdr.credentials, HELLO_Header::CREDS_SIZE_V1);
          break;
        }
        case AuthenticationType::SSL: {
          X509* cert = w->sender().getPeerCert(from);
          // cert can be nullptr. it is handled by getPrincipal
          // use 1 as size as is not used for SSL cetficate
          principal = principal_parser->getPrincipal(cert, 1);
          // X509_free handles nullptr
          X509_free(cert);
          break;
        }
        case AuthenticationType::NONE:
        case AuthenticationType::MAX:
          ld_critical("INTERNAL ERROR: Cluster has invalid PrincipalParser of "
                      "type MAX or NONE");
          // This should never happen. Authentication should not be enabled
          // if the PrincipalParser is of type NONE or MAX
          ld_check(false);
          fill_out_client_info_in_principal();
          return principal;
      }
    }

    fill_out_client_info_in_principal();

    if (principal.type == Principal::INVALID) {
      ackhdr.status = E::ACCESS;
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      3,
                      "ACCESS ERROR: Got a HELLO message with an invalid "
                      "authentication data that could not be parsed.");
      return principal;
    }

    // we will reject when we require a credential, and one is not supplied
    if (!Worker::getConfig()->serverConfig()->allowUnauthenticated() &&
        principal.type == Principal::UNAUTHENTICATED) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      3,
                      "ACESS ERROR: Got a HELLO message with out "
                      "authentication data when cluster requries "
                      "authentication.");
      ackhdr.status = E::ACCESS;
      return principal;
    }

    int rv = w->sender().setPrincipal(from, principal);
    if (rv != 0) {
      ld_critical("INTERNAL ERROR: Could not set principal for HELLO Message "
                  "received from %s",
                  Sender::describeConnection(from).c_str());
      // This should never happen. We are invoking onReceived and thus
      // this function from the Socket that needs its principal set.
      ld_check(false);
      ackhdr.status = E::INTERNAL;
    }
  }

  return principal;
}

/**
 * Send a reply o the sender.
 *
 * @param ackhdr      Header of the ACK message to send as a reply.
 * @param from        Address of the recipient.
 * @param build_info  Build information provided by the client
 * @param Disposition that onReceived() should return.
 *                    Set to Message::Disposition::ERROR if there was an error,
 *                    otherwise set to Message::Disposition::NORMAL.
 * @return Message::Disposition::ERROR in case of an error or ackhdr.status is
 *         != E::OK, and err is set to ackhdr.status.
 *         return Message::Disposition::NORMAL otherwise.
 */
static Message::Disposition
sendReply(ACK_Header& ackhdr,
          const Address& from,
          const bool should_log,
          const PrincipalIdentity& principal,
          const folly::Optional<folly::dynamic>& build_info) {
  const auto reject_hello = Worker::settings().reject_hello;
  if (reject_hello != E::OK) {
    ld_info("--test-reject-hello is set on the command line. "
            "Rejecting a HELLO from %s with status %s.",
            Sender::describeConnection(from).c_str(),
            error_name(reject_hello));
    ackhdr.status = reject_hello;
  }

  ld_check(from.isClientAddress());

  auto ack = std::make_unique<ACK_Message>(ackhdr);
  auto worker = Worker::onThisThread();
  int rv = worker->sender().sendMessage(std::move(ack), from);

  if (rv != 0) {
    ld_check_in(err, ({E::NOMEM, E::INTERNAL, E::SHUTDOWN}));
    ld_error("Failed to acknowledge a new connection from  %s. error %s. "
             "Closing connection.",
             Sender::describeConnection(from).c_str(),
             error_description(err));
    ackhdr.status = E::INTERNAL;
  }

  // log the handshake result
  if (should_log) {
    auto conn_type = worker->sender().getSockConnType(from);
    ClientHelloInfoTracer logger(worker->getTraceLogger());

    logger.traceClientHelloInfo(
        build_info, principal, conn_type, ackhdr.status);
  }

  if (ackhdr.status != E::OK) {
    ld_check_in(ackhdr.status,
                ({E::BADMSG,
                  E::PROTONOSUPPORT,
                  E::ACCESS,
                  E::INVALID_CLUSTER,
                  E::INTERNAL,
                  E::DESTINATION_MISMATCH}));
    err = ackhdr.status;
    return Message::Disposition::ERROR;
  }

  return Message::Disposition::NORMAL;
}

Message::Disposition HELLO_Message::onReceived(const Address& from) {
  ACK_Header ackhdr{0, header_.rqid, from.id_.client_.getIdx(), 0, E::OK};

  if (!from.isClientAddress()) {
    ld_error("PROTOCOL ERROR: got a HELLO from the passive side of "
             "connection %s. Closing.",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  if (header_.proto_min == 0 || header_.proto_max == 0 ||
      header_.proto_min > header_.proto_max) {
    ld_error("Bad HELLO from %s: protocol range [%hu, %hu] is invalid. "
             "Closing connection.",
             Sender::describeConnection(from).c_str(),
             header_.proto_min,
             header_.proto_max);
    err = E::BADMSG;
    return Disposition::ERROR;
  }

  uint16_t proto = ackhdr.proto;
  auto r = checkProto(header_.proto_min, header_.proto_max, &proto);
  ackhdr.proto = proto;
  if (r != 0) {
    ld_check(err == E::PROTONOSUPPORT);
    ld_error("Got a HELLO from %s which supports protocols in range "
             "[%hu, %hu] but this server expects a version in range "
             "[%hu, %hu]. Rejecting connection.",
             Sender::describeConnection(from).c_str(),
             header_.proto_min,
             header_.proto_max,
             Compatibility::MIN_PROTOCOL_SUPPORTED,
             Worker::settings().max_protocol);
    ackhdr.status = E::PROTONOSUPPORT;
  }

  if (header_.flags & HELLO_Header::SOURCE_NODE) {
    if (!source_node_id_.isNodeID()) {
      ld_error("Got a HELLO from %s with SOURCE_NODE flag and an invalid "
               "NodeID",
               Sender::describeConnection(from).c_str());
      err = E::BADMSG;
      return Disposition::ERROR;
    }

    Worker::onThisThread()->sender().setPeerNodeID(from, source_node_id_);
  }

  // must be called after we check if the connection is from a source node.
  auto principal = checkAuthenticationData(header_, ackhdr, from, csid_);

  if (header_.flags & HELLO_Header::DESTINATION_NODE) {
    if (!destination_node_id_.isNodeID()) {
      ld_error("Got a HELLO from %s with DESTINATION_NODE flag and an "
               "invalid NodeID",
               Sender::describeConnection(from).c_str());
      err = E::BADMSG;
      return Disposition::ERROR;
    }

    auto my_node_id = Worker::onThisThread()->processor_->getMyNodeID();
    if (destination_node_id_ != my_node_id) {
      ld_error("Got a HELLO from %s with a DESTINATION_NODE which does not "
               "match this node. Destination NodeID: %s. My NodeID: %s.",
               Sender::describeConnection(from).c_str(),
               destination_node_id_.toString().c_str(),
               my_node_id.toString().c_str());
      err = E::DESTINATION_MISMATCH;
      return Disposition::ERROR;
    }
  }

  if (header_.flags & HELLO_Header::CLUSTER_NAME) {
    std::string server_cluster_name =
        Worker::getConfig()->serverConfig()->getClusterName();

    if (server_cluster_name != cluster_name_) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      3,
                      "Got a HELLO from %s with CLUSTER_NAME flag with"
                      "mismatching cluster_names. "
                      "Server Cluster Name: %s, "
                      "Client Cluster Name: %s",
                      Sender::describeConnection(from).c_str(),
                      server_cluster_name.c_str(),
                      cluster_name_.c_str());
      ackhdr.status = E::INVALID_CLUSTER;
    }
  }

  if (header_.flags & HELLO_Header::CSID) {
    int rv = Worker::onThisThread()->sender().setCSID(from, csid_);
    if (rv != 0) {
      ld_critical("INTERNAL ERROR: Could not set CSID for HELLO Message "
                  "received from %s",
                  Sender::describeConnection(from).c_str());
      // This should never happen.
      ld_check(false);
      ackhdr.status = E::INTERNAL;
    }
  }

  if (header_.flags & HELLO_Header::CLIENT_LOCATION) {
    if (!NodeLocation::validDomain(client_location_)) {
      RATELIMIT_ERROR(std::chrono::seconds(5),
                      1,
                      "Got a HELLO from %s with an invalid location (%s)",
                      Sender::describeConnection(from).c_str(),
                      client_location_.c_str());
      err = E::BADMSG;
      return Disposition::ERROR;
    }
    Worker::onThisThread()->sender().setClientLocation(
        from.id_.client_, client_location_);
  }

  // Parse extra build information if provided
  folly::Optional<folly::dynamic> build_info;
  if (!(header_.flags & HELLO_Header::SOURCE_NODE) &&
      header_.flags & HELLO_Header::BUILD_INFO) {
    try {
      build_info = folly::parseJson(build_info_);
    } catch (std::exception& e) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "Got a HELLO message from %s with "
                      "invalid client build info, failed to parse json: %s",
                      Sender::describeConnection(from).c_str(),
                      e.what());
      ackhdr.status = E::BADMSG;
    }
  }

  return sendReply(ackhdr,
                   from,
                   !(header_.flags & HELLO_Header::SOURCE_NODE),
                   principal,
                   build_info);
}

MessageReadResult HELLO_Message::deserialize(ProtocolReader& reader) {
  std::unique_ptr<HELLO_Message> message(new HELLO_Message());
  reader.read(const_cast<HELLO_Header*>(&message->header_));

  if (message->header_.flags & HELLO_Header::SOURCE_NODE) {
    reader.read(&message->source_node_id_);
    if (reader.ok() && !message->source_node_id_.isNodeID()) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: Got a HELLO message with an invalid "
                      "NodeID");
      return reader.errorResult(E::BADMSG);
    }
  }

  if (message->header_.flags & HELLO_Header::DESTINATION_NODE) {
    reader.read(&message->destination_node_id_);
    if (reader.ok() && !message->destination_node_id_.isNodeID()) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: Got a HELLO message with an invalid "
                      "NodeID");
      return reader.errorResult(E::BADMSG);
    }
  }

  // If the CLUSTER_NAME flag is set, read the length of the cluster_name_ off
  // the evbuffer (look at serialize() for more info). The length is then
  // used to read the cluster_name_ off of the evbuffer.
  if (message->header_.flags & HELLO_Header::CLUSTER_NAME) {
    uint16_t cluster_name_len = 0;
    reader.read(&cluster_name_len);

    if (reader.ok() &&
        cluster_name_len > configuration::ZookeeperConfig::MAX_CLUSTER_NAME) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: Got a HELLO message with the "
                      "length of the cluster name being longer than "
                      "ZookeeperConfig::MAX_CLUSTER_NAME");
      return reader.errorResult(E::BADMSG);
    }

    std::string buf(cluster_name_len, '\0');
    reader.read(&buf[0], buf.size());
    message->cluster_name_ = std::move(buf);
    if (reader.ok() &&
        !configuration::parser::validClusterName(message->cluster_name_)) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: Got a HELLO message with an invalid "
                      "Cluster Name: %s",
                      message->cluster_name_.c_str());
      return reader.errorResult(E::BADMSG);
    }
  }

  // If the CSID flag is set, read the length of the csid_ off
  // the evbuffer (look at serialize() for more info). The length is then
  // used to read the csid_ off of the evbuffer.
  if (message->header_.flags & HELLO_Header::CSID) {
    uint16_t csid_len = 0;
    reader.read(&csid_len);

    if (reader.ok() && csid_len > MAX_CSID_SIZE) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: Got a HELLO message with the "
                      "length of the csid being longer than "
                      "MAX_CSID_LEN (%u)",
                      csid_len);
      return reader.errorResult(E::BADMSG);
    }

    std::string buf(csid_len, '\0');
    reader.read(&buf[0], buf.size());
    message->csid_ = std::move(buf);
  }

  // When this flag is set, client reports extra information such as
  // build version, builder, build path, ..
  // as a json blob preceded with a 2-byte length prefix
  if (message->header_.flags & HELLO_Header::BUILD_INFO) {
    uint16_t json_len = 0;
    reader.read(&json_len);
    if (!reader.ok()) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: Got a HELLO message with "
                      "invalid client build info length prefix");
      return reader.errorResult(E::BADMSG);
    }

    // Read the actual length-prefixed string
    std::string client_json_str(json_len, '\0');
    reader.read(&client_json_str[0], client_json_str.size());
    message->build_info_ = std::move(client_json_str);

    if (!reader.ok()) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: Got a HELLO message with "
                      "invalid client build info");
      return reader.errorResult(E::BADMSG);
    }
  }

  // If the CLIENT_LOCATION flag is set, read the length of the client_location_
  // from the evbuffer (look at serialize() for more info).
  // Read 'length' bytes corresponding to client_location_ from evbuffer.
  if (message->header_.flags & HELLO_Header::CLIENT_LOCATION) {
    reader.readLengthPrefixedVector(&message->client_location_);

    if (!reader.ok()) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: Got a HELLO message with "
                      "invalid client location");
      return reader.errorResult(E::BADMSG);
    }
  }

  // There may be more bytes belonging to this message in the evbuffer. This
  // can happen if the protocol was extended and our peer uses the newer
  // version we don't understand yet.
  reader.allowTrailingBytes();

  return reader.resultMsg(std::move(message));
}

void HELLO_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);

  if (header_.flags & HELLO_Header::SOURCE_NODE) {
    ld_check(source_node_id_.isNodeID() &&
             "SOURCE_NODE flag is set, but source_node_id_ is not valid");
    writer.write(source_node_id_);
  }

  if (header_.flags & HELLO_Header::DESTINATION_NODE) {
    ld_check(destination_node_id_.isNodeID() &&
             "DESTINATION_NODE flag is set, but destination_node_id_ is not "
             "valid");
    writer.write(destination_node_id_);
  }

  // include the size of the cluster name and the cluster name itself in the
  // message
  if (header_.flags & HELLO_Header::CLUSTER_NAME) {
    ld_check(cluster_name_.size() != 0 &&
             cluster_name_.size() < std::numeric_limits<uint16_t>::max());

    uint16_t cluster_name_length = cluster_name_.size();
    writer.write(cluster_name_length);
    writer.writeVector(cluster_name_);
  }

  if (header_.flags & HELLO_Header::CSID) {
    uint16_t csid_length = csid_.size();
    writer.write(csid_length);
    writer.writeVector(csid_);
  }

  if (header_.flags & HELLO_Header::BUILD_INFO) {
    ld_check(build_info_.size() != 0 &&
             build_info_.size() < std::numeric_limits<uint16_t>::max());

    uint16_t build_info_length = build_info_.size();
    writer.write(build_info_length);
    writer.writeVector(build_info_);
  }

  if (header_.flags & HELLO_Header::CLIENT_LOCATION) {
    writer.writeLengthPrefixedVector(client_location_);
  }
}

}} // namespace facebook::logdevice
