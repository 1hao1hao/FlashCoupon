SELECT merchant_id,id,stock,remaining,
 (SELECT COUNT(*) FROM grants g WHERE g.merchant_id=t.merchant_id AND g.template_id=t.id AND g.state='issued') AS issued
 FROM control.templates t;
SELECT 'ds0_t0' AS shard,COUNT(*) AS coupons FROM coupon_0.coupons_0
UNION ALL SELECT 'ds0_t1',COUNT(*) FROM coupon_0.coupons_1
UNION ALL SELECT 'ds1_t0',COUNT(*) FROM coupon_1.coupons_0
UNION ALL SELECT 'ds1_t1',COUNT(*) FROM coupon_1.coupons_1;
SELECT 'ds0_t0' AS shard,COUNT(*) AS templates FROM coupon_0.merchant_templates_0
UNION ALL SELECT 'ds0_t1',COUNT(*) FROM coupon_0.merchant_templates_1
UNION ALL SELECT 'ds1_t0',COUNT(*) FROM coupon_1.merchant_templates_0
UNION ALL SELECT 'ds1_t1',COUNT(*) FROM coupon_1.merchant_templates_1;
SELECT COUNT(*) AS oversold FROM control.templates WHERE remaining<0 OR remaining>stock;
SELECT COUNT(*) AS user_limit_violations FROM control.user_limits u
JOIN control.templates t ON t.merchant_id=u.merchant_id AND t.id=u.template_id WHERE u.issued>t.per_user;
