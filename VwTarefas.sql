create view VwTarefas as 
SELECT
    [Session ID]=er.session_id,  
    [User Process]=es.is_user_process,  
    [Status]=er.status,  
    [Command] =er.command,  
    [Database]=db_name(er.database_id),  
	[Blocking Session ID]=er.blocking_session_id,
	[Wait Type]=er.wait_type,
	[Wait Time]=er.wait_time,
	[Last Wait Type]=er.last_wait_type,
	[Wait Resource]=er.wait_resource,
	[Open Transaction Count]=er.open_transaction_count,
	[Open Resultset Count]=er.open_resultset_count,
	[Percent Complete]=er.percent_complete,
	[CPU Time]=er.cpu_time,
	[Total Elapsed Time]=er.total_elapsed_time,
	[Reads]=er.reads,
	[Writes]=er.writes,
  [Start Time]=er.start_time,
  es.[host_name],
  [Program Name]=es.program_name,
  [Login Name]=es.login_name,
  [NT User Name]=es.nt_user_name,
  [Estimated Completion Time]=er.estimated_completion_time,
	[Logical Reads]=er.logical_reads,
	[Query]=SUBSTRING(qt.text, er.statement_start_offset/2, 
					(CASE WHEN er.statement_end_offset = -1 
					THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2 
                    ELSE er.statement_end_offset END - er.statement_start_offset)/2), 
    [Batch]=text,
    [Plan]=qp.query_plan,
	[StatementStartOffset]=er.statement_start_offset,
	[StatementEndOffset]=er.statement_end_offset,
	[SqlHandle]=er.sql_handle, 
	[PlanHandle]=er.plan_handle
FROM sys.dm_exec_requests er 
JOIN sys.dm_exec_sessions es ON er.session_id = es.session_id 
OUTER APPLY sys.dm_exec_sql_text(er.sql_handle)as qt 
OUTER APPLY sys.dm_exec_query_plan(er.plan_handle) qp 
WHERE er.session_Id NOT IN (@@SPID) AND es.is_user_process = 1

GO

