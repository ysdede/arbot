# -*- coding: utf8 -*-

import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gAuth


# get the path of ChromeDriverServer
dir = os.path.dirname(__file__)
chrome_driver_path = dir + "\chromedriver.exe"

# create a new Chrome session
driver = webdriver.Chrome(chrome_driver_path)
driver.implicitly_wait(20)
driver.maximize_window()
driver.execute_script("document.body.style.zoom='60%'")

driver.get("https://www.foobar")

btnGirisYap = driver.find_element_by_xpath("//*[contains(text(), 'GİRİŞ YAP')]")
btnGirisYap.click()

wait = WebDriverWait(driver, 5)

login_form = driver.find_element_by_name('LoginForm')
txtboxTelefon = driver.find_element_by_css_selector('[ng-model="app.loginModel.UserPhone"]')
txtboxSifre = driver.find_element_by_css_selector('[ng-model="app.loginModel.UserPassword"]')
btnLogin = driver.find_element_by_css_selector('[ng-click="app.Login($event)"]')

txtboxTelefon.send_keys('foo')
txtboxSifre.send_keys('bar')
btnLogin.click()
txtbox2FA = driver.find_element_by_css_selector('[ng-model="app.VerificationCode2FA"]')
btn2FA = driver.find_element_by_css_selector('[ng-click="app.Verification2FA()"]')
new2FA = gAuth.getAuth()
txtbox2FA.send_keys(new2FA)

btn2FA.click()

lists= driver.find_elements_by_class_name("r")
print ("Found " + str(len(lists)) + " searches:")

i=0
for listitem in lists:
   print (listitem.get_attribute("innerHTML"))
   i=i+1
   if(i>10):
      break
