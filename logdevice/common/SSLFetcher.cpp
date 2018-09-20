/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SSLFetcher.h"

#include <folly/portability/OpenSSL.h>

// Values for supported identity certificate types
#define HOST_IDENTITY_CERT_TYPE 0
#define SERVICE_IDENTITY_CERT_TYPE 1
#define USER_IDENTITY_CERT_TYPE 2

// 0x80 is the return value for ASN1_get_object() when any error occurs
#define OPENSSL_ASN1_GET_OBJECT_ERROR 0x80

namespace facebook { namespace logdevice {

const char* SSLFetcher::IDENTITY_TYPE_OID = "1.3.6.1.4.1.40981.2.2.5";
const char* SSLFetcher::BASIC_CONSTRAINTS_OID = "2.5.29.19";

int SSLFetcher::verify_callback(int preverify_ok, X509_STORE_CTX* x509_ctx) {
  // This callback is called after openssl does verification on the
  // cert, result of that is stored in preverify_ok. If the initial
  // verification was successful there is no need to do more processing.
  if (preverify_ok == 1)
    return preverify_ok;

  int ssl_status = X509_STORE_CTX_get_error(x509_ctx);
  int verify_cert = 0;

  switch (ssl_status) {
    // We only expect this one error as RootCanal identity certificates
    // have a critical extension that must be checked.
    case X509_V_ERR_UNHANDLED_CRITICAL_EXTENSION: {
      X509* cert = X509_STORE_CTX_get_current_cert(x509_ctx);
      ld_check(cert);

      for (int i = 0; i < X509_get_ext_count(cert); i++) {
        X509_EXTENSION* ex = X509_get_ext(cert, i);
        // extension should be valid
        ld_check(ex);

        // we only care about critical extensions
        if (X509_EXTENSION_get_critical(ex) == 0) {
          continue;
        }

        // Get OID of extension. OBJ_obj2txt will produce a null terminated
        // string in oid_buffer, truncating the OID if necessary. OpenSSL
        // documentation states that a 80 char buffer should be enough to handle
        // any OID.
        char oid_buffer[80];
        int size =
            OBJ_obj2txt(oid_buffer, 80, X509_EXTENSION_get_object(ex), 1);
        if (size <= 0) {
          ld_error("Could not extract OID from critical extension");
          return 0;
        }

        if (strcmp(oid_buffer, BASIC_CONSTRAINTS_OID) == 0) {
          // This is checked in the default verification.
          continue;
        } else if (strcmp(oid_buffer, IDENTITY_TYPE_OID) == 0) {
          // IDENTITY_TYPE_OID represents the source of the identity cert.
          // We will only accept identity certificates from Host,
          // Tupperware Services and Users. Any other identity cert will be
          // rejected

          long int len = ASN1_STRING_length(X509_EXTENSION_get_data(ex));
          int type, xclass;
          const unsigned char* ex_data =
              ASN1_STRING_get0_data(X509_EXTENSION_get_data(ex));

          if (ex_data == nullptr) {
            ld_error("No data for extension %s", oid_buffer);
            return 0;
          }

          // Moves the ex_data ptr past the header information of the ASN1
          // encoded string. Modifies len to represent the new value of
          // ex_data without the header information.
          int rv = ASN1_get_object(&ex_data, &len, &type, &xclass, len);

          if (rv == OPENSSL_ASN1_GET_OBJECT_ERROR ||
              type != V_ASN1_UTF8STRING) {
            ld_error("Could not parse data in extension %s", oid_buffer);
            return 0;
          }

          bool valid_extension_data = true;
          int cert_type;
          try {
            size_t pos;

            // The data of the extension should only consist of numbers.
            // If non numeric values exists within the data, the extensions
            // data should be considered invalid.
            cert_type = std::stoi(reinterpret_cast<const char*>(ex_data), &pos);

            // If the entire length is not parsed by stoi then there is
            // non-numeric character in the extension data
            if (pos != len) {
              valid_extension_data = false;
            }
          } catch (std::invalid_argument&) {
            valid_extension_data = false;
          }

          if (!valid_extension_data) {
            ld_error("Invalid data in extension: %s. Only numeric "
                     "values are expected.",
                     IDENTITY_TYPE_OID);
            return 0;
          }

          // Only accepts certificates from host, tw services and
          // dev boxes. Other identity certificates types are not
          // supported by logdevice and should be rejected.
          if (cert_type == HOST_IDENTITY_CERT_TYPE ||
              cert_type == SERVICE_IDENTITY_CERT_TYPE ||
              cert_type == USER_IDENTITY_CERT_TYPE) {
            verify_cert = 1;
          } else {
            ld_error("Error occurred during SSL verification. "
                     "Invalid identity certificate type used: %d",
                     cert_type);
          }
        } else {
          ld_error("Did not check critical extension %s", oid_buffer);
          return 0;
        }
      }
      break;
    }
    default:
      ld_error("Error occurred during SSL verification. "
               "error code: %d",
               ssl_status);
  }
  return verify_cert;
}

}} // namespace facebook::logdevice
