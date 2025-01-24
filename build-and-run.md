docker build -t etl-pipeline .
docker run --env-file .env etl-pipeline


python src/pipelines/etl.py --reference_date 2023-01-01
