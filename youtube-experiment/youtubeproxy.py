from youtubecrawler import get_my_ip, get_ip_details, get_webpage, WEB_PAGE_URL, FUNCTION_REGION

def main():
    my_ip = get_my_ip()
    my_ip_details = get_ip_details(my_ip)
    my_country = my_ip_details[1]
    webpage, timestamp = get_webpage()

    return str(dict(
        url=WEB_PAGE_URL,
        html_content=webpage,
        ip_address_of_request=my_ip,
        country_of_request=my_country + '-' + FUNCTION_REGION,
        timestamp_of_request=timestamp
    ))

if __name__=='__main__':
    main()