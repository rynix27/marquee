.PHONY: install seed test run deploy clean

install:
	pip install -r requirements.txt --break-system-packages

seed:
	python -m backend.test_seed

test:
	python -m backend.test_seed
	python -m backend.test_alerts
	python -m backend.test_tts

run:
	uvicorn backend.main:app --reload --port 8080

deploy:
	./scripts/deploy.sh

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
