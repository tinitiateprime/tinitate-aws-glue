import utils as u

lookup_data = [{"feed_name":"products"
                ,"data_soruce_bucket_folder_name":"s3://abc/data/""
                ,"crawler_target_bucket_folder_name":
                ,"crawler_name": ,'categories'}, # 0
                {"feed_name":"products"
                ,"data_soruce_bucket_folder_name":"s3://abc/data/""
                ,"crawler_target_bucket_folder_name":
                ,"crawler_name": ,'categories'}, #1
                {"feed_name":"products"
                ,"data_soruce_bucket_folder_name":"s3://abc/data/""
                ,"crawler_target_bucket_folder_name":
                ,"crawler_name": ,'categories'}  #2
                 ]                             


for data in lookup_data:
    u.create_crawler(s3_source_data =  data["data_soruce_bucket_folder_name"]