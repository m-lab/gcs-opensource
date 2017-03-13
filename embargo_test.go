package gcs

import (
	"fmt"
	"testing"
)

func TestGetLocalIP(t *testing.T) {
	result1 := GetLocalIP("20170225T23:00:00Z_4.34.58.34_0.web100.gz")
	if result1 != "4.34.58.34" {
		t.Errorf("wrong! %v\n", result1)
		return
	}

	result2 := GetLocalIP("20170225T23:00:00Z_ALL0.web100.gz")
	if result2 != "" {
		t.Errorf("wrong! %v\n", result2)
		return
	}
	fmt.Printf("GetLocalIP Correct!\n")
}

func TestReadWhitelistFromLocal(t *testing.T) {
	whiteList := ReadWhitelistFromLocal("whitelist")
	if whiteList["213.244.128.170"] {
		fmt.Printf("ReadWhitelist correct\n")
	} else {
		fmt.Printf("wrong\n")
	}
	if whiteList["2001:4c08:2003:2::16"] {
		fmt.Printf("wrong\n")
	} else {
		fmt.Printf("ReadWhitelist correct\n")
	}
	return
}

func TestReadWhitelistFromGCS(t *testing.T) {
	whiteList := ReadWhitelistFromGCS("whitelist")
	if whiteList["213.244.128.170"] {
		fmt.Printf("ReadWhitelist correct\n")
	} else {
		fmt.Printf("wrong\n")
	}
	if whiteList["2001:4c08:2003:2::16"] {
		fmt.Printf("wrong\n")
	} else {
		fmt.Printf("ReadWhitelist correct\n")
	}
	return
}

func TestEmbargoCheck(t *testing.T) {
	whitelist := ReadWhitelistFromLocal("whitelist")
	// After embargo data and IP not whitelisted. Return true, embargoed
	if EmbargoCheck("20170225T23:00:00Z_4.34.58.34_0.web100.gz", whitelist) {
		fmt.Printf("EmbargoCheck correct\n")
	} else {
		fmt.Printf("wrong\n")
	}

	// After embargo data and IP whitelisted. Return false, not embargoed
	if !EmbargoCheck("20170225T23:00:00Z_213.244.128.170_0.web100.gz", whitelist) {
		fmt.Printf("EmbargoCheck correct\n")
	} else {
		fmt.Printf("wrong\n")
	}
	// Before embargo data. Return false, not embargoed
	if !EmbargoCheck("20150225T23:00:00Z_213.244.128.1_0.web100.gz", whitelist) {
		fmt.Printf("EmbargoCheck correct\n")
	} else {
		fmt.Printf("wrong\n")
	}
	return
}

func TestEmbargo(t *testing.T) {
	if Embargo() {
		fmt.Printf("Embargo correct\n")
	} else {
		t.Error("wrong\n")
	}
	return
}
