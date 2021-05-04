package util

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

type Signer struct {
}

func (h *Signer) GenerateSignature(jwtConfig *JwtConfig, key string) string {
	signer := hmac.New(sha256.New, []byte(jwtConfig.SecretKey))
	signer.Write([]byte(key))
	return hex.EncodeToString(signer.Sum(nil))
}

func (h *Signer) ValidateSignature(jwtConfig *JwtConfig, key string, eSignature string) bool {
	cSignature := h.GenerateSignature(jwtConfig, key)
	return cSignature == eSignature
}
