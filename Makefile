.PHONY: install up down topics sample dbt test dashboard clean

install:
	python -m pip install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down

topics:
	python scripts/bootstrap_topics.py

sample:
	python scripts/generate_bronze_sample.py

dbt:
	mkdir -p data/lakehouse
	dbt run --project-dir dbt --profiles-dir dbt && dbt test --project-dir dbt --profiles-dir dbt

test:
	python -m pytest

dashboard:
	streamlit run dashboard/app.py

clean:
	rm -rf data dbt/target dbt/logs .pytest_cache
