postgres:
	docker run --name transaction-system-postgres-15 -p 5432:5432 -e POSTGRES_USER=root -e POSTGRES_PASSWORD=root -d postgres:15

create_db:
	docker exec -it transaction-system-postgres-15 createdb --username=root --owner=root transaction-system-pg

drop_db:
	docker exec -it transaction-system-postgres-15 dropdb transaction-system-pg

.PHONY: postgres create_db drop_db