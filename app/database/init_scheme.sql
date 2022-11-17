CREATE TABLE "account" (
                           "account_id" bigserial PRIMARY KEY,
                           "balance" numeric(12,2) NOT NULL
);

INSERT INTO "account"(balance) VALUES (1245);
INSERT INTO "account"(balance) VALUES (0);
INSERT INTO "account"(balance) VALUES (3465);
INSERT INTO "account"(balance) VALUES (222);
INSERT INTO "account"(balance) VALUES (673456);