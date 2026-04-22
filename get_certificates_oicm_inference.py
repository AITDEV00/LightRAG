import ssl
import socket
import requests
from cryptography import x509
from cryptography.x509.oid import ExtensionOID, AuthorityInformationAccessOID
from cryptography.hazmat.primitives import serialization

# Updated Target details
hostname = "inference.adeoaiengine.ecouncil.ae"
ip = "10.34.104.100"
port = 443

def get_aia_issuer_url(cert_der):
    """Extract the CA Issuers URL from the AIA extension of a certificate."""
    try:
        cert = x509.load_der_x509_certificate(cert_der)
        aia = cert.extensions.get_extension_for_oid(ExtensionOID.AUTHORITY_INFORMATION_ACCESS).value
        # Look for the CA_ISSUERS access method and an HTTP URI
        for access in aia:
            if access.access_method == AuthorityInformationAccessOID.CA_ISSUERS:
                uri = access.access_location.value
                if isinstance(uri, str) and uri.startswith("http"):
                    return uri
    except Exception:
        pass
    return None

def is_self_signed(cert_der):
    """Check if a certificate is self-signed (Root)."""
    try:
        cert = x509.load_der_x509_certificate(cert_der)
        return cert.subject == cert.issuer
    except Exception:
        return False

print(f"Connecting to {ip} (SNI: {hostname}) over port {port}...")

# Create an unverified context so it doesn't drop the connection due to trust errors
context = ssl._create_unverified_context()

try:
    # Connect via IP, but pass the hostname for SNI (Server Name Indication)
    with socket.create_connection((ip, port), timeout=10) as sock:
        with context.wrap_socket(sock, server_hostname=hostname) as ssock:
            
            # get_unverified_chain() requires Python 3.10+
            # It pulls the exact certificate chain the server sends during the handshake
            chain = list(ssock.get_unverified_chain())
            
            if not chain:
                print("No certificates were returned by the server.")
            else:
                print(f"Successfully retrieved {len(chain)} certificate(s) from server handshake.")
                
                # AIA Discovery Loop
                if not is_self_signed(chain[-1]):
                    print("\nDiscovering missing certificate chain via AIA...")
                    while True:
                        last_cert_der = chain[-1]
                        if is_self_signed(last_cert_der):
                            print("Reached Root CA (self-signed). Discovery complete.")
                            break
                            
                        aia_url = get_aia_issuer_url(last_cert_der)
                        if not aia_url:
                            print("No more AIA 'CA Issuers' URLs found. Discovery stopped.")
                            break
                            
                        print(f"Following AIA URL: {aia_url}")
                        try:
                            import urllib.request
                            with urllib.request.urlopen(aia_url, timeout=30) as response:
                                new_cert_der = response.read()
                                chain.append(new_cert_der)
                        except Exception as e:
                            print(f"Failed to download certificate from AIA: {e}")
                            break
                
                print(f"\nFinal chain size: {len(chain)} certificate(s). Saving files...\n")
                
                for index, cert_der in enumerate(chain):
                    # Convert the binary DER format to text-based PEM format
                    cert_pem = ssl.DER_cert_to_PEM_cert(cert_der)
                    
                    # Name the files based on their position in the chain
                    if index == 0:
                        cert_type = "Leaf/Server"
                        filename = f"{hostname}_Leaf.cer"
                    elif index == len(chain) - 1 and is_self_signed(cert_der):
                        cert_type = "Root CA"
                        filename = f"{hostname}_Root.cer"
                    else:
                        cert_type = f"Intermediate CA {index}"
                        filename = f"{hostname}_Intermediate_{index}.cer"
                        
                    # Save the certificate to the current directory
                    with open(filename, "w") as cert_file:
                        cert_file.write(cert_pem)
                        
                    print(f"[{cert_type}] Saved to: {filename}")

except AttributeError:
    print("Error: Your Python version does not support 'get_unverified_chain()'. Please use Python 3.10 or newer.")
except Exception as e:
    print(f"An error occurred: {e}")