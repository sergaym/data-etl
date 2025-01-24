1. ahora mismo todo está en data/, pero lo normal es que tuvieras en S3 o semejante los readings y en otra db los datos de sqlite. Por tanto, habría que mejorar el código para que pueda leer de S3 y de la db sqlite.

2. En ese proceso de lectura, supongo que lo tendríamos en base al tiempo, por lo que en ese proceso de lectura para automatizarlo haría una lambda que consulta un periodo concreto, por ejemplo yesterday data y que se ejecuta al día siguiente.

3. Podría ser una lambda que ejecuta ETL.py, que es el que ejecuta el ETL. Y el que trigerea ese pipeline.

4. Qué pasa con el cumsum?! Habría que consultar la db para ver the latest value y hacer el cumsum desde ese valor. Revisa bien el concepto de cumsum de la tabla y qué hay que hacer exactamente.

5. El pipeline de ETL está pensado para ingestar datos, no para consultarlos. Para eso, habría que hacer una consulta a la db. Tenemos un caso de ejmplo con el streamlit y con sqls directamente en analyst_sql.py.
