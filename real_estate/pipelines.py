# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import psycopg2
from itemadapter import ItemAdapter


class RealEstatePipeline:
    def process_item(self, item, spider):
        return item


class RealEstatePipeline:
    def open_spider(self, spider):
        # Kết nối tới container db2 (Data Warehouse)
        try:
            self.conn = psycopg2.connect(
                host="db2",
                database="real_estate_db",
                user="root",
                password="admin",
                port="5432"
            )
            self.curr = self.conn.cursor()
            spider.logger.info("Connected to PostgreSQL (db2) successfully")
        except Exception as e:
            spider.logger.error(f"Error connecting to PostgreSQL: {e}")
            raise e

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # Hàm hỗ trợ ép kiểu để tránh lỗi SQL khi dữ liệu cào về bị trống (None)
        def to_float(val):
            try: return float(val) if val is not None else None
            except: return None

        def to_int(val):
            try: return int(val) if val is not None else None
            except: return None

        # Câu lệnh Insert khớp 100% với các cột của bảng properties
        insert_query = """
            INSERT INTO properties (
                title, price, price_unit, area, area_unit, 
                district, city, bedroom, bathroom, news_type, 
                posted_date, expired_date, seller, total_posts, 
                url, zalo_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data = (
            adapter.get('title'),
            to_float(adapter.get('price')),
            adapter.get('price_unit'),
            to_float(adapter.get('area')),
            adapter.get('area_unit'),
            adapter.get('district'),
            adapter.get('city'),
            to_int(adapter.get('bedroom')),
            to_int(adapter.get('bathroom')),
            adapter.get('news_type'),
            adapter.get('posted_date'), # Định dạng YYYY-MM-DD
            adapter.get('expired_date'),
            adapter.get('seller'),
            to_int(adapter.get('total_posts')),
            adapter.get('url'),
            adapter.get('zalo_url')
        )

        try:
            self.curr.execute(insert_query, data)
            self.conn.commit()
        except Exception as e:
            spider.logger.error(f"Failed to insert item into DB: {e}")
            self.conn.rollback() # Rollback để tránh hỏng transaction

        return item

    def close_spider(self, spider):
        if hasattr(self, 'curr'):
            self.curr.close()
        if hasattr(self, 'conn'):
            self.conn.close()