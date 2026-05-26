-- Seed enough stock that the happy-path saga always reserves successfully.
-- SKUs match the ones in samples/sample-order.json and the integration tests.
-- Week-5 compensation tests will deliberately request more than what's seeded to
-- exercise the InventoryRejected branch.

INSERT INTO inventory (sku, seller_id, on_hand, reserved, version) VALUES
    ('sku-1',              'seller-a',        1000, 0, 0),
    ('sku-2',              'seller-b',        1000, 0, 0),
    ('sku-z',              'seller-z',        1000, 0, 0),
    ('ACME-WIDGET-001',    'seller-acme',     1000, 0, 0),
    ('GLOBEX-GADGET-007',  'seller-globex',   1000, 0, 0);
