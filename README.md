Datamart ETL
============


DETL is a custom Extract Trasform Load engine for [eTools](https://github.com/unicef/etools). 
It automatically move from _multi-tenant_ design to single tenant,
preserving exisiting foreignkeys and constraints.

It is able to automatically inspect existing database and react to schema updates.

Final database will be exposed thru [eTools Datamart](https://github.com/unicef/etools-datamart.git/) and can be investigated using
[Datamart SuperSet](https://github.com/unicef/datamart-superset.git/)  
