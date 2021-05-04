package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"github.com/dgrijalva/jwt-go"
	"github.com/spf13/viper"
	"io"
	"time"
)

type Claims struct {
	EntityId string
	jwt.StandardClaims
}

const (
	accessTokenExpiryTimeUnit  = time.Hour
	refreshTokenExpiryTimeUnit = time.Hour
)

type AuthHelper struct {
}

func (ah *AuthHelper) GenerateAccessRefreshKeyPair(userId string) (map[string]string, error) {
	accessToken, err := ah.createToken(userId, time.Now().Add(time.Duration(viper.GetInt("jwt_config.access_token_expiry_duration"))*accessTokenExpiryTimeUnit))
	if err != nil {
		return nil, err
	}
	refreshToken, err := ah.createToken(userId, time.Now().Add(time.Duration(viper.GetInt("jwt_config.refresh_token_expiry_duration"))*refreshTokenExpiryTimeUnit))
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"access_token":  accessToken,
		"refresh_token": refreshToken,
	}, nil
}

func (ah *AuthHelper) createToken(userId string, expirationTime time.Time) (string, error) {
	var err error
	claims := &Claims{
		EntityId: userId,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: expirationTime.Unix(),
		},
	}
	at := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token, err := at.SignedString([]byte(viper.GetString("jwt_config.secret_key")))
	if err != nil {
		return "", err
	}
	return token, nil
}

func (ah *AuthHelper) ValidateTokenExpiry(token string) (*Claims, bool) {
	claims := &Claims{}
	at, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte(viper.GetString("jwt_config.secret_key")), nil
	})
	if err != nil || !at.Valid {
		return nil, false
	}
	return claims, true
}

func (ah *AuthHelper) EncryptAES(text string) (string, error) {
	textInBytes := []byte(text)
	block, err := aes.NewCipher([]byte(viper.GetString("jwt_config.cipher_key")))
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
