
```
pip install virtualenv

virtualenv venv
source venv/bin/activate
which python #to verify 
```

```
pip install jupyter 
jupyter notebook
jupyter lab   # preferred
```

-  once running jupyter lab or notebook, to view in UI
```
docker pull apache/spark-py  //one time


docker run -it --rm apache/spark-py /opt/spark/binpyspark     //PySpark shell will start

http://localhost:4040
```