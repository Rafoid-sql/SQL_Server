create view VwSessoes as select db_name(database_id) banco,	* from sys.dm_exec_sessions where database_id > 4;
GO

