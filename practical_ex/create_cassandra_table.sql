USE fc_catalog;
CREATE COLUMNFAMILY central_catalog (
    product_id int,
    item_id int,
    price int,
    product_name varchar,
    original_item_title varchar,
    free_shipping boolean,
    item_create_timestamp timestamp,
    item_update_timestamp timestamp,
    review_count int,
    review_mean_score double,
    search_tag list<varchar>,
    seller_id int,
    seller_name varchar,
    attrs map<varchar, varchar>,
    categories map<int, varchar>,
    promotions map<int, varchar>,
    PRIMARY KEY(product_id, item_id)
);