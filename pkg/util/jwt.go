package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"github.com/dgrijalva/jwt-go"
	"io"
	"time"
)

type JwtConfig struct {
	CipherKey                string
	SecretKey                string
	AccessKeyExpiryTimeUnit  time.Duration
	RefreshKeyExpiryTimeUnit time.Duration
}

type Claims struct {
	EntityId string
	jwt.StandardClaims
}

type AuthHelper struct {
}

func (ah *AuthHelper) GenerateAccessRefreshKeyPair(jwtConfig *JwtConfig, userId string) (map[string]string, error) {
	accessToken, err := ah.createToken(jwtConfig, userId, time.Now().Add(jwtConfig.AccessKeyExpiryTimeUnit))
	if err != nil {
		return nil, err
	}
	refreshToken, err := ah.createToken(jwtConfig, userId, time.Now().Add(jwtConfig.RefreshKeyExpiryTimeUnit))
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"access_token":  accessToken,
		"refresh_token": refreshToken,
	}, nil
}

func (ah *AuthHelper) createToken(jwtConfig *JwtConfig, userId string, expirationTime time.Time) (string, error) {
	var err error
	claims := &Claims{
		EntityId: userId,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: expirationTime.Unix(),
		},
	}
	at := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token, err := at.SignedString([]byte(jwtConfig.SecretKey))
	if err != nil {
		return "", err
	}
	return token, nil
}

func (ah *AuthHelper) ValidateTokenExpiry(jwtConfig *JwtConfig, token string) (*Claims, bool) {
	claims := &Claims{}
	at, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte(jwtConfig.SecretKey), nil
	})
	if err != nil || !at.Valid {
		return nil, false
	}
	return claims, true
}

func (ah *AuthHelper) EncryptAES(jwtConfig *JwtConfig, text string) (string, error) {
	textInBytes := []byte(text)
	block, err := aes.NewCipher([]byte(jwtConfig.CipherKey))
	if err != nil {
		return "", err
	}
	b := base64.StdEncoding.EncodeToString(textInBytes)
	ciphertext := make([]byte, aes.BlockSize+len(b))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}
	cfb := cipher.NewCFBEncrypter(block, iv)
	cfb.XORKeyStream(ciphertext[aes.BlockSize:], []byte(b))
	return text, nil
}
