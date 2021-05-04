package util

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"github.com/spf13/viper"
)

type Signer struct {
}

func (h *Signer) GenerateSignature(key string) string {
	signer := hmac.New(sha256.New, []byte(viper.GetString("encryption_config.key")))
	signer.Write([]byte(key))
	return hex.EncodeToString(signer.Sum(nil))
}

func (h *Signer) ValidateSignature(key string, eSignature string) bool {
	cSignature := h.GenerateSignature(key)
	return cSignature == eSignature
}
