import scrapy
import numpy as np

class bdsSpider(scrapy.Spider):
    name = "real_estate.spiders"
    start_urls = ["https://batdongsan.com.vn/cho-thue-nha-tro-phong-tro-ha-noi"]

    def parse(self, response):
        cards = response.xpath("//div[contains(@class, 'js__card js__card-full-web')]")

        for card in cards:
            
            title = card.xpath(".//span[contains(@class, 'pr-title js__card-title')]/text()").get()
            price = card.xpath(".//span[contains(@class,'re__card-config-price js__card-config-item')]/text()").get()
            area = card.xpath(".//span[contains(@class,'re__card-config-area js__card-config-item')]/text()").get()
            address=card.xpath("normalize-space(.//div[contains(@class, 're__card-location')])").get()
            # url = card.xpath(".//a[contains(@class, 'js__product-link-for-product-id')]/@href").get()
            bedroom = card.xpath(".//span[contains(@class, 're__card-config-bedroom')]/@aria-label").get()
            bathroom = card.xpath(".//span[contains(@class, 're__card-config-toilet js__card-config-item')]/@aria-label").get()
            posted_date = card.xpath(".//span[contains(@class, 're__card-published-info-published-at')]/@aria-label").get()
            # seller = card.xpath(".//div[contains(@class, 're__contact-name')]/text()").get()
            # detail_info = card.xpath("normalize-space(.//div[contains(@class, 're__card-description js__card-description')])").get()
            url = card.xpath(".//a[contains(@class, 'js__product-link-for-product-id')]/@href").get()
            data = {
                "title":title.strip() if title else ("updating"),
                "price":price.strip() if price else np.nan,
                "area":area.strip()if area else np.nan,
                "address":address.strip()if address else ("updating"),
                # "url":url,
                "bedroom":bedroom.strip() if bedroom else None,
                "bathroom":bathroom.strip() if bathroom else None,
                "posted_date":posted_date.strip() if posted_date else None,
                "url": response.urljoin(url).strip() if url else None
              
            }
            

            # url = card.xpath(".//a[contains(@class, 'js__product-link-for-product-id')]/@href").get()
            if url:
                yield scrapy.Request(
                    url = response.urljoin(url),
                    callback = self.parse_detail,
                    meta = {'data':data}
                )

        next_page = response.xpath("//a[contains(@class, 're__pagination-icon')][last()]/@href").get()
        if next_page:
            self.logger.info(f"Đang chuyển sang trang: {next_page}")
            yield scrapy.Request(
                url=response.urljoin(next_page),
                meta={'use_curl_cffi': True},
                callback=self.parse  
            )

    def parse_detail(self, response):
        data = response.meta['data']
        data['expired_date'] = response.xpath("//div[contains(@class, 're__pr-short-info-item') and .//span[contains(text(), 'Ngày hết hạn')]]/span[@class='value']/text()").get()
        data['news_type'] = response.xpath("//div[contains(@class, 're__pr-short-info-item') and .//span[contains(text(), 'Loại tin')]]/span[@class='value']/text()").get()
        data['seller'] = response.xpath("normalize-space(.//a[contains(@class, 're__contact-name')])").get()
        data['zalo_url'] = response.xpath("//a[contains(@class, 'js__zalo-chat')]/@data-href").get()
        data['total_posts'] = response.xpath("//a[contains(text(), 'Xem thêm')]/text()").get()
        yield data

        
 
  



   