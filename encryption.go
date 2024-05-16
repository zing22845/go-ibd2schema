package ibd2schema

const (
	/** Encryption magic bytes size */
	MAGIC_SIZE = 3
	/** Encryption key length */
	KEY_LEN = 32
	/** UUID of server instance, it's needed for composing master key name */
	SERVER_UUID_LEN = 36
	/** Encryption information total size: magic number + master_key_id +
	  key + iv + server_uuid + checksum */
	INFO_SIZE = (MAGIC_SIZE + SIZE_OF_UINT32 + (KEY_LEN * 2) + SERVER_UUID_LEN +
		SIZE_OF_UINT32)
	/** Maximum size of Encryption information considering all
	  formats v1, v2 & v3. */
	INFO_MAX_SIZE = INFO_SIZE + SIZE_OF_UINT32
)
