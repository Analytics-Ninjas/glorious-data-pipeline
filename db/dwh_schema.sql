CREATE TABLE stock_dim
(
     SK_stock_id SERIAL PRIMARY KEY NOT NULL,
	 stock_id   VARCHAR(5) NOT NULL,
     company    VARCHAR(100) NOT NULL,
     category   VARCHAR(100) NOT NULL,
     price      DECIMAL(10, 2) NOT NULL,
     start_date TIMESTAMP NOT NULL,
     end_date   TIMESTAMP NOT NULL,
     is_current CHAR(1) NOT NULL,
     CONSTRAINT company UNIQUE (company)
);

CREATE TABLE user_dim
(
    SK_user_id SERIAL PRIMARY KEY NOT NULL,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(100) NOT NULL,
    updated_at TIMESTAMP,
    status     VARCHAR(1) DEFAULT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date   TIMESTAMP NOT NULL,
    is_current CHAR(1) NOT NULL
);

CREATE TABLE transaction_fact
(
    SK_transaction_id   SERIAL PRIMARY KEY,
    FK_user_id          INT NULL,
    FK_stock_id         INT NOT NULL,
    quantity         	INT NOT NULL,
    transaction_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT transaction_stock_stock_id_fk FOREIGN KEY (FK_stock_id) REFERENCES
    stock_dim (SK_stock_id),
    CONSTRAINT transaction_user_user_id_fk FOREIGN KEY (FK_user_id) REFERENCES
    user_dim (SK_user_id)
);
