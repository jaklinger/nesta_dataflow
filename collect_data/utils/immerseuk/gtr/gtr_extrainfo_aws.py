from selenium import webdriver
from selenium.webdriver.common.keys import Keys

def run(event,context):
    driver = webdriver.PhantomJS(service_args=['--ssl-protocol=any'])
    driver.implicitly_wait(10)
    driver.get('http://www.python.org/')
    assert "Python" in driver.title
    elem = driver.find_element_by_name("q")
    elem.send_keys("pycon")
    elem.send_keys(Keys.RETURN)
    assert "No results found." not in driver.page_source
    print(driver.title)
    driver.quit() 
