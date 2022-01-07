package github.com/bnc-dev/go-library

import (
	"archive/zip"
	"crypto/tls"
	"fmt"
	"io"
	"net/smtp"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	cryptomd5 "crypto/md5"
	email "github.com/jordan-wright/email"
)

func IsNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func IsInteger(s string) bool {
	_, err := strconv.Atoi(s)
	return err == nil
}

func ToFloat64(s string, default_value float64) float64 {
	var lnValue float64 = default_value
	tmp, err := strconv.ParseFloat(s, 64)
	if(err == nil){
		lnValue = tmp
	}
	return lnValue
}

func ToInteger64(s string, default_value int64) int64 {
	var liValue int64 = default_value
	tmp, err := strconv.ParseInt(s, 10, 64)
	if(err == nil){
		liValue = tmp
	}
	return liValue
}

func ToDate(s string) time.Time {
	var ldtValue time.Time
	
	re_dmy := regexp.MustCompile("(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[012])/((19|20)\\d\\d)")
	re_ymd := regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	if(re_dmy.MatchString(s)){
		layout := "02/01/2006"
		str := s
		tmp, err := time.Parse(layout, str)
		if(err == nil){
			ldtValue = tmp
		}
	}else if(re_ymd.MatchString(s)){
		layout := "2006-01-02"
		str := s
		tmp, err := time.Parse(layout, str)
		if(err == nil){
			ldtValue = tmp
		}
	}
	
	return ldtValue
}

func MD5(s string) string {
	data := []byte(s)
	return fmt.Sprintf("%x",cryptomd5.Sum(data))
}

func GetFieldString(e interface{}, field string) string {
	r := reflect.ValueOf(e)
	f := reflect.Indirect(r).FieldByName(field)
	return f.String()
}

func GetFieldInteger(e interface{}, field string) int {
	r := reflect.ValueOf(e)
	f := reflect.Indirect(r).FieldByName(field)
	return int(f.Int())
}

func InStringSlice(val string, slice []string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

func SendMail(param map[string]interface{}) error {
	var result_error error

	sender_host, _ := param["sender_host"]
	sender_port, _ := param["sender_port"]
	sender_name, _ := param["sender_name"]
	sender_email, _ := param["sender_email"]
	sender_password, _ := param["sender_password"]
	sender_tls, _ := param["sender_tls"]
	sender_insecure_skip_verify, _ := param["sender_insecure_skip_verify"]
	
	arr_to, _ := param["to"]
	arr_cc, _ := param["cc"]
	arr_bcc, _ := param["bcc"]
	arr_attachment, _ := param["attachment"]
	
	ls_subject, _ := param["subject"]
	ls_message_text, _ := param["message"]
	ls_message_html, _ := param["message_html"]

	e := email.NewEmail()

	var ls_sender_host string = ""
	var ls_sender_port string = ""
	var ls_sender_email string = ""
	var ls_sender_password string = ""
	var ls_sender_name string = ""
	var ls_sender_tls string = ""
	var ls_sender_insecure_skip_verify bool = true
	
	if(sender_host != nil && strings.TrimSpace(sender_host.(string)) != ""){
		ls_sender_host = sender_host.(string)
	}

	if(sender_port != nil && strings.TrimSpace(sender_port.(string)) != ""){
		ls_sender_port = sender_port.(string)
	}

	if(sender_email != nil && strings.TrimSpace(sender_email.(string)) != ""){
		ls_sender_email = sender_email.(string)
	}
	
	if(sender_name != nil && strings.TrimSpace(sender_name.(string)) != ""){
		ls_sender_name = sender_name.(string)
	}

	if(sender_password != nil && strings.TrimSpace(sender_password.(string)) != ""){
		ls_sender_password = sender_password.(string)
	}else{
		ls_sender_password = "U2FsdGVkX19yCR2w7jntHnP9fHrCC+WDcwHDgyq/Njg="
	}

	if(sender_tls != nil && strings.TrimSpace(sender_tls.(string)) != ""){
		ls_sender_tls = sender_tls.(string)
	}else{
		ls_sender_tls = "tls"
	}

	if(sender_insecure_skip_verify != nil && strings.TrimSpace(sender_insecure_skip_verify.(string)) != "" && strings.TrimSpace(sender_insecure_skip_verify.(string)) != "true"){
		ls_sender_insecure_skip_verify = false
	}else{
		ls_sender_insecure_skip_verify = true
	}

	e.From = (ls_sender_name + " <" + ls_sender_email + ">")

	if(arr_to != nil && len(arr_to.([]string)) > 0){
		var arr_tmp []string = []string{}
		for _, val :=  range arr_to.([]string) {
			if(strings.TrimSpace(val) != ""){
				arr_tmp =  append(arr_tmp, val)
			}
		}
		e.To = arr_tmp
	}

	
	if(arr_cc != nil && len(arr_cc.([]string)) > 0){
		var arr_tmp []string = []string{}
		for _, val :=  range arr_cc.([]string) {
			if(strings.TrimSpace(val) != ""){
				arr_tmp =  append(arr_tmp, val)
			}
		}
		e.Cc = arr_tmp
	}

	if(arr_bcc != nil && len(arr_bcc.([]string)) > 0){
		var arr_tmp []string = []string{}
		for _, val :=  range arr_bcc.([]string) {
			if(strings.TrimSpace(val) != ""){
				arr_tmp =  append(arr_tmp, val)
			}
		}
		e.Bcc = arr_tmp
	}

	if(arr_attachment != nil && len(arr_attachment.([]string)) > 0){
		for _, val :=  range arr_attachment.([]string) {
			if(strings.TrimSpace(val) != ""){
				e.AttachFile(val)
			}
		}
		
	}


	e.Subject = ls_subject.(string)
	
	if(ls_message_text != nil && strings.TrimSpace( ls_message_text.(string)) != ""){
		e.Text = []byte(ls_message_text.(string))
	}

	if(ls_message_html != nil && strings.TrimSpace( ls_message_html.(string)) != ""){
		e.HTML = []byte(ls_message_html.(string))
	}
	
	
	/// TLS config
	tlsconfig := &tls.Config {
		InsecureSkipVerify: ls_sender_insecure_skip_verify,
		ServerName: ls_sender_host,
	}
	_=tlsconfig

	if(ls_sender_tls == "tls") {
		result_error = e.SendWithTLS(ls_sender_host+":"+ls_sender_port, smtp.PlainAuth("", ls_sender_email, Decrypt(ls_sender_password), ls_sender_host), tlsconfig)
	}else if(ls_sender_tls == "starttls") {
		result_error = e.SendWithStartTLS(ls_sender_host+":"+ls_sender_port, smtp.PlainAuth("", ls_sender_email, Decrypt(ls_sender_password), ls_sender_host), tlsconfig)
	}else {
		result_error = e.Send(ls_sender_host + ":" + ls_sender_port, smtp.PlainAuth("", ls_sender_email, Decrypt(ls_sender_password), ls_sender_host))
	}
	// fmt.Println("result_error", result_error)

	return result_error

}


func Unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer rc.Close()

		fpath := filepath.Join(dest, f.Name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(fpath, f.Mode())
		} else {
			var fdir string
			if lastIndex := strings.LastIndex(fpath,string(os.PathSeparator)); lastIndex > -1 {
				fdir = fpath[:lastIndex]
			}

			err = os.MkdirAll(fdir, f.Mode())
			if err != nil {
				return err
			}
			f, err := os.OpenFile(
				fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer f.Close()
			
			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func RemoveDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	err = os.Remove(dir)
	if err != nil {
		return err
	}
	return nil
}

func CopyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
			return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}