-module(tasktracker).
-export([
	 start/2,
	 task_tracker/2,
	 yoyo/0,
	 yoyo/1	 
	 ]).

-import(mapper,
	[ mapper/6 ]).

-import(reducer,
	[ reducer/5 ]).


%% yoyo server is here only for debugging purposes
yoyo()->
    register(yoyo, spawn(tasktracker,yoyo,[loop])).

yoyo(loop) ->
    receive 
	_ ->
	    yoyo(loop)
    end.


%% Main thread initialiser
%% Spawn off the tasktracker.
start(Reg_name, Job_tracker) ->
    register( Reg_name, spawn(tasktracker, task_tracker, 
			      [discovery, Job_tracker]) ).



%% The task tracker - Discovery stage
%% pings the main server which has the jobtracker 
%% NOTE: THE NODE RUNNING JOBTRACKER MUST BE NAMED GIVEN AS ARG
task_tracker(discovery, Job_tracker) ->
    io:format("task_tracker : Discovery phase, Attempting contact with ~p~n",
	     [Job_tracker]),
    case net_adm:ping(Job_tracker) of
	pong ->
	    io:format("task_tracker : Job_tracker found~n"),
	    task_tracker(discovered, [{jobtracker, Job_tracker}|[[ready]]] );
	pang ->
	    receive
		%% if the old Job_tracker dies out, we may have these nodes
		%% spawned off, so we may send a new job tracker nodes name
		%% and continue with the new job tracker
		{new_job_tracker, New_job_tracker} ->
		    task_tracker(discovery, New_job_tracker)
	    after 5000 ->
		    task_tracker(discovery, Job_tracker)
	    end
    end;	

%% Node has been discovered.
%% we move to the job accept stage.
%% If we don't receive a broadcast for 12s (broadcast spacing is 5s)
%%   we move to the undiscovered stage // we have lost contact with the
%%                                        jobtracker.  
task_tracker(discovered, Status) ->            
    receive

	%% Currently no checks are made to check if the original Job_tracker 
	%% node is the one from which we are receiving broadcasts from
	%% If support for multiple job_trackers are needed, it will go here.
	{job_tracker_live, _} ->	   	    
%%	    io:format("tast_tracker: Received broadcast from Job_tracker ~p~n",
%%		      [Job_tracker]),
	    task_tracker(discovered, Status );
	
	%% Stop mapper/reducer if running
	{global_die} ->
	    [ _ | [[Stat]|_] ] = Status,
	    if 
		Stat =/= active ->
		    mapper ! {die}
	    end,	          	    
	    io:format("~ntask_tracker dying.. ~n");
	    

	%% Request for spawning mappers
	%% 1. Spawn off mappers
	%% 2. Update status loopback
	{job_tracker_map, Mapperfunc, Pattern, Num_mappers, Num_reducers}
	->
	    [{jobtracker, JTnode}|[[Stat]|_]] = Status,
	    case Stat of  
		ready ->		    
		    Input_files = filelib:wildcard(Pattern),    
		    rpc:sbcast([JTnode],jTracker,{files,node(),Input_files}),
		    register('mapper', spawn(mapper, mapper, 
					     [Mapperfunc, 
					      [Input_files,[],[],[]],
					      Num_mappers,	      
					      Num_reducers,
					      self(),
					      0
					     ])),
		    io:format("~n Received Map Job~n"),
		    task_tracker(discovered,     %% Task_tracker status  
				 [{jobtracker,JTnode}, %% Job_tracker details   
				  [active],      %% Current status
				  [Input_files, [], []], %% Input files 
				  [[], [], []],  %% Intermediate files
				  []             %% Result files
				 ]
				);
		_ ->
		    io:format("~nIllegal attempt : Mappers in use")
	    end;
	
	%% Update status and loop task_tracker
	%% send result update to job_tracker
	%% Filename = filename of the input file which is processed
	%% 
	{mapper_result_success, Filename, Inter_files} 
	->
	    %%io:format("~n File:~p processed, New file-> ~p",
	%%	      [Filename, Inter_files]),
	    [{jobtracker,JTnode}, 
	     [_], 
	     [Input_mfiles, Done_mfiles, Bad_mfiles],
	     [Int_rfiles, Done_rfiles, Bad_rfile],
	     Resultfiles
	    ] = Status,
	    rpc:sbcast([JTnode],jTracker,{mapper_result_success, 
					  node(), Filename, Inter_files}),
	    task_tracker(discovered,   
			 [{jobtracker,JTnode} ,     
			  [active],    
			  [Input_mfiles -- Filename,
			   Done_mfiles ++ Filename,
			   Bad_mfiles],
			  [Int_rfiles ++ Inter_files, Done_rfiles, Bad_rfile],
			  Resultfiles
			 ]
			);	
	    
	
	%% Mapper is finished processing all files matching the input pattern
	{mapper_complete}
	->
	    [{jobtracker,JTnode}, 
	     [_], 
	     [Input_mfiles, Done_mfiles, Bad_mfiles],
	     [Int_rfiles, Done_rfiles, Bad_rfile],
	     Resultfiles
	    ] = Status,
	    rpc:sbcast([JTnode], jTracker, {mapper_complete, node()}),
	    io:format("~n Mapping complete! "),
	    task_tracker(discovered,   
			 [{jobtracker,JTnode} ,     
			  [active],    
			  [Input_mfiles, Done_mfiles, Bad_mfiles],
			  [Int_rfiles , Done_rfiles, Bad_rfile],
			  Resultfiles
			 ]
			);	
	
    	     
	{mapper_result_failure, Filename} 
	->
	    [{jobtracker,JTnode},
	     [_], 
	     [Input_mfiles, Done_mfiles, Bad_mfiles],
	     [Int_rfiles, Done_rfiles, Bad_rfile],
	     Resultfiles
	    ] = Status,
	    task_tracker(discovered,   
			 [{jobtracker,JTnode},
			  [active],    
			  [Input_mfiles -- Filename, 
			   Done_mfiles , 
			   Bad_mfiles ++ Filename],
			  [Int_rfiles , Done_rfiles, Bad_rfile],
			  Resultfiles
			 ]
			),
	    rpc:sbcast([JTnode],jTracker,{mapper_result_success, 
				      node(), Filename});

	%% Request for spawning a 
	%% The reduce function and an Id is passed to the job
	{reducer_job_spawn, Redfunc, Id}->
	    
	    [{jobtracker,JTnode} | [[State]|T] ] = Status,
	    [Num] = io_lib:format("~p",[Id]),
	    Name = list_to_atom("reducer" ++ Num),

	    io:format("~nSpawning Reducer ~p on Node ~p~n",[Name, node()]),
	    
	    register( Name, spawn(reducer,reducer,
				[Redfunc,
				 [],
				 self(),
				 Id,
				 [[],[]]
				])),
	    task_tracker(discovered,   
			 [{jobtracker,JTnode}|[ [active] | T]]
			 );

	%% This here justs acts as a relay
	{reduce_files, Files, Id} ->	    
	    [Num] = io_lib:format("~p",[Id]),
	    Name = list_to_atom("reducer" ++ Num),
	    Name ! {reduce_files, Files, Id},
	    task_tracker(discovered, Status);
	

	%% Request from the jobtracker to get results from the Reducer
	{reduce_return, Id} ->
	    [Num] = io_lib:format("~p",[Id]),
	    Name = list_to_atom("reducer" ++ Num),
	    Name ! {reduce_return, Id},
	    task_tracker(discovered, Status);	

	%% Reply from the reducer.
	%% relay filename to the jobtracker
	{reduce_return_filename, Id,Filename, [Done,Bad]} ->	    

	    [{jobtracker,JTnode}|_] = Status,

	    rpc:sbcast([JTnode], jTracker, 
		       {reduce_return_filename, Id, node(),
			Filename, [Done,Bad]}),
	    
	    task_tracker(discovered, Status);	
	
	{die} ->
	    io:format("task_tracker: Exiting...~n");
	
	Any ->
	    io:format("~ntask_tracker: Received a weird message ~p~n",[Any]),
	    task_tracker(discovered, Status)
    after 11000 ->	    	  
	    [{jobtracker, JTracker}|[ [State] |_ ]] = Status,
	    case State of
		ready ->
		    task_tracker(discovery, JTracker);
		active ->		    
		    task_tracker(discovered, Status);
		done ->
		    task_tracker(discovery, JTracker)
	    end
    end.
