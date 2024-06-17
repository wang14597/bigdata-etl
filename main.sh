export PYTHONPATH=$(pwd)/src
cd src/generate_properties
python main.py ../../conf/data-migration-config
cd ../..
python src/main.py conf/data-migration-config /etl/jars/etl-tools.jar