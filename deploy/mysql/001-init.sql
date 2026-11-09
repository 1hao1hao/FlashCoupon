CREATE DATABASE IF NOT EXISTS control;
CREATE DATABASE IF NOT EXISTS coupon_0;
CREATE DATABASE IF NOT EXISTS coupon_1;
USE control;
CREATE TABLE templates (
 merchant_id BIGINT NOT NULL, id VARCHAR(64) NOT NULL, name VARCHAR(120) NOT NULL,
 stock BIGINT NOT NULL, remaining BIGINT NOT NULL, per_user BIGINT NOT NULL,
 threshold_amount BIGINT NOT NULL, discount BIGINT NOT NULL,
 starts_at BIGINT NOT NULL, ends_at BIGINT NOT NULL, valid_until BIGINT NOT NULL,
 mode VARCHAR(16) NOT NULL, status VARCHAR(16) NOT NULL, redis_initialized BOOLEAN NOT NULL DEFAULT FALSE,
 PRIMARY KEY(merchant_id,id)
) ENGINE=InnoDB;
CREATE TABLE grants (
 id CHAR(64) PRIMARY KEY, merchant_id BIGINT NOT NULL, template_id VARCHAR(64) NOT NULL,
 user_id BIGINT NOT NULL, state VARCHAR(16) NOT NULL, reason VARCHAR(255) NOT NULL DEFAULT '',
 coupon_json JSON NULL, accepted_at BIGINT NOT NULL, created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 KEY template_grants(merchant_id,template_id,state), KEY user_grants(user_id,template_id)
) ENGINE=InnoDB;
CREATE TABLE user_limits (
 merchant_id BIGINT NOT NULL, template_id VARCHAR(64) NOT NULL, user_id BIGINT NOT NULL, issued BIGINT NOT NULL,
 PRIMARY KEY(merchant_id,template_id,user_id)
) ENGINE=InnoDB;
CREATE TABLE batches (
 id VARCHAR(64) PRIMARY KEY, merchant_id BIGINT NOT NULL, template_id VARCHAR(64) NOT NULL,
 state VARCHAR(16) NOT NULL, total BIGINT NOT NULL DEFAULT 0, accepted BIGINT NOT NULL DEFAULT 0,
 rejected BIGINT NOT NULL DEFAULT 0, error_text TEXT, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;
CREATE TABLE batch_items (
 batch_id VARCHAR(64) NOT NULL, user_id BIGINT NOT NULL, event_id CHAR(64) NULL, state VARCHAR(16) NOT NULL, error_text VARCHAR(255) NOT NULL DEFAULT '',
 PRIMARY KEY(batch_id,user_id)
) ENGINE=InnoDB;
-- Same schema is also available in control for the unsharded development profile.
CREATE TABLE coupons (
 user_id BIGINT NOT NULL, id CHAR(64) NOT NULL, merchant_id BIGINT NOT NULL, template_id VARCHAR(64) NOT NULL,
 threshold_amount BIGINT NOT NULL, discount BIGINT NOT NULL, expires_at BIGINT NOT NULL,
 status VARCHAR(16) NOT NULL, order_id VARCHAR(96) NULL,
 PRIMARY KEY(user_id,id), UNIQUE KEY order_once(user_id,order_id), KEY expiry(status,expires_at), KEY user_merchant(user_id,merchant_id,status)
) ENGINE=InnoDB;
CREATE TABLE coupon_0.coupons_0 LIKE control.coupons;
CREATE TABLE coupon_0.coupons_1 LIKE control.coupons;
CREATE TABLE coupon_1.coupons_0 LIKE control.coupons;
CREATE TABLE coupon_1.coupons_1 LIKE control.coupons;
-- Merchant-oriented read projection. No distributed transaction with the user projection.
CREATE TABLE merchant_templates LIKE templates;
CREATE TABLE coupon_0.merchant_templates_0 LIKE templates;
CREATE TABLE coupon_0.merchant_templates_1 LIKE templates;
CREATE TABLE coupon_1.merchant_templates_0 LIKE templates;
CREATE TABLE coupon_1.merchant_templates_1 LIKE templates;
