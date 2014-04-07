import base64, os

from Crypto.Hash import SHA
from Crypto.Cipher import AES  
from Crypto.PublicKey import RSA
from Crypto import Random
from Crypto.Signature import PKCS1_PSS
import jwt
    
private_key = None
public_key = None
signatory = None
verifier = None
secret = None

def verify(string, signature):
    h = SHA.new()
    h.update(string)
    return verifier.verify(h, signature)
    
def sign(string):
    h = SHA.new()
    h.update(string)
    return signatory.sign(h)
    
def hex_hash(string):
    h = SHA.new()
    h.update(string)
    return h.hexdigest()      
    
def pad(string, block_size):
    pad = block_size - len(string) % block_size #pad is between 1 and block_size, never 0
    return string + pad * chr(pad) #last character tells you how many pad characters

def durable_encrypt(string):
    return encrypt(string, lock)
    
def durable_decrypt(encrypted_string):
    return decrypt(encrypted_string, lock)
    
def encrypt(string, key=None):
    # key is expected to be 32 bytes, e.g. key = os.urandom(32)
    if key is None:
        key = secret
    padded_string = pad(string, AES.block_size)
    iv = os.urandom(AES.block_size)
    encrypted_bytes = iv + AES.new(key, AES.MODE_CBC, iv).encrypt(padded_string)
    encrypted_string = base64.urlsafe_b64encode(str(encrypted_bytes))
    return encrypted_string
    
def decrypt(encrypted_string, key=None):
    # key is expected to be 32 bytes, e.g. key = os.urandom(32)
    if key is None:
        key = secret
    encrypted_bytes = base64.urlsafe_b64decode(encrypted_string)
    iv = encrypted_bytes[:AES.block_size]
    encrypted_bytes = encrypted_bytes[AES.block_size:]
    plain_text = AES.new(key, AES.MODE_CBC, iv).decrypt(encrypted_bytes)
    pad = ord(plain_text[-1])
    return plain_text[:-pad]

SIGNATURE_PUBLIC_KEY = 'our little secret'

def encode_jwt(claims, alg='HS256'):
    return jwt.encode(claims, SIGNATURE_PUBLIC_KEY, alg)
                                       
def decode_jwt(signature, verify_expiration=True):
    try:
        return jwt.decode(signature, SIGNATURE_PUBLIC_KEY, verify_expiration=verify_expiration)
    except jwt.ExpiredSignature:
        return None
    
def generate_new_keys():
    rng = Random.new().read
    private_key = RSA.generate(1024, rng)
    secret = os.urandom(32)
    return (private_key, secret)
    
private_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../private')
lock = base64.urlsafe_b64decode('BwFujPI4xn93s77hS0cP4QbyIuWqy8owAURVI9muAis=')

def create_key_files():
    priv_key, secrt = generate_new_keys()
    f = open(os.path.join(private_dir,'privatekey.pem'),'w')
    f.write(encrypt(priv_key.exportKey('PEM'), secrt))
    f = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../data/publickey.pem'),'w')
    f.write(priv_key.publickey().exportKey('PEM'))
    f = open(os.path.join(private_dir,'secret.key'),'w')
    f.write(encrypt(secrt, lock))
    f.close()

def read_key_files():
    global private_key
    global public_key
    global signatory
    global verifier
    global secret
    try:
        f = open(os.path.join(private_dir,'secret.key'),'r')   
        encrypted_secret = f.read()
        secret = decrypt(encrypted_secret, lock)
        f = open(os.path.join(private_dir,'privatekey.pem'),'r')
        encrypted_private_key = f.read()
        candidate_private_key = decrypt(encrypted_private_key, secret)
        if candidate_private_key.startswith('-----BEGIN RSA PRIVATE KEY-----'):
            private_key = RSA.importKey(candidate_private_key)
            public_key = private_key.publickey()
            signatory = PKCS1_PSS.new(private_key)
            verifier = PKCS1_PSS.new(public_key)
        f.close()
    except IOError:
        pass # print 'could not open key file'
        
read_key_files()