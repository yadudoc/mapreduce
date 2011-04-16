-module(jobtracker).
-export([
	 start/1,
%%	 broadcast/1,
	 broadcaster/3,
	 submit_job/0,
	 jtracker/5
	 ]).


% Broadcast message to all nodes.
broadcast(Reg_name)->    
    rpc:sbcast(Reg_name, {job_tracker_live, node()} ).

% This is part of the Node discovery phase
% Repeatedly broadcast every set duration to identify new nodes
% We only check for new nodes. 
% NOTE : WE ARE NOT HANDLING NODES GOING DOWN HERE
start(Reg_name)->    
    %% spawn the jobtracker with registered name mr_jobtracker
    register( 'bCaster' , spawn(jobtracker,broadcaster, 
				    [Reg_name, 5000, []]
			       )),
    io:format("~n Jobtracker: Broadcaster is Up!"),
    
    register( 'jTracker', spawn(jobtracker, jtracker, 
				[Reg_name, 5000, [], 
				 [ [[],[]], [[],[]], [] ], 
				 []
				]
			       ) ),
    io:format("~n Jobtracker: Jobtracker main thread is Up!").
    
    

broadcaster(Reg_name, Time, Good_old) ->

    {Good_new, _} = broadcast(Reg_name),
    New_node = Good_new -- Good_old,
    if 
	%% Case 1 : new nodes found !
	New_node =/= [] ->
	    jTracker ! {new_node, New_node};
	true ->
	    %%jTracker ! {no_new_node}
	    ok
    end,
    %% The following is a lame sleep implementation
    %% we want the broadcast only in a few minutes
    receive
	{global_die} ->
	    rpc:sbcast(Reg_name, {global_die}),
	    io:format("~n Broadcaster dying..");
		      
	{die} ->
	    io:format("~n Broadcaster dying..")
    after Time ->
	    broadcaster(Reg_name, Time, Good_new)
    end.	      


%% listens and handles all notifications on node discovery
%%  status, node lost etc.
%% Functions is of type [ [Mapper,Inputpatte], [Reducer,N_reducers] ]
%% Files is of type [ [[ Input_files_todo ], [Input_files_done]],
%%                    [[ Inter_files_todo ], [Inter_files_done]],  
%%                    [ Output_file_done ]]
%% Nodes is a list of ready/active nodes

jtracker(Reg_name, Time, Functions, Files, Nodes) ->
    io:format("~njtracker looping ~n"),
    receive 
	{die} ->
	    io:format("~n jTracker exiting...");

	%% Bit of a security issue right here.
	%% anybody could technically send a global_die message
	{global_die} ->
	    bCaster ! {global_die},
	    io:format("~n Jobtracker dying..");
	
	%% This is pretty useless, but lets us know the broadcaster is alive
	{no_new_node} ->
	    jtracker(Reg_name, Time, Functions, Files, Nodes);
	
	%% Notifies us of a new node.
	%% TODO: when new node joins start map job.
	{new_node, New_node} ->
	    io:format("~n New node(s) found! ->  ~p~n",[New_node]),	    
	    jtracker(Reg_name, Time, Functions, Files, Nodes++New_node );
	
		
	%% Allow new requests only when the job is completed !	
	%%  
	%% We add a new job_request here.
	%% First the map is done on all available nodes
	%% We spawn Num_reducers number of reducers and
	%% keep them waiting till mappers return results
	{job_request, Mapfunc, Inp_pattern, Num_mappers, 
	 Redfunc, Num_reducers} when Functions =:= [] ->
	    io:format("~n Requesting map job at nodes ~p",[Nodes]),

	    rpc:sbcast(Nodes, Reg_name,
		       {job_tracker_map, Mapfunc, Inp_pattern, 
			Num_mappers, Num_reducers}
		      ),
	    
	    R_todo = roundrobin(Nodes, lists:seq(0,Num_reducers-1)),
	    io:format("~nScheduling scheme : ~p~n",[R_todo]),	    

	    lists:map( fun([N,Id]) ->
			       rpc:sbcast([N], Reg_name,
					  {reducer_job_spawn, 
					   fun(X)->X end,
						%Redfunc, 
					   Id})
		       end,
		       R_todo ),
	    
	    jtracker(Reg_name,Time, 	                       
		     [[Mapfunc, [[N,active]|| N<-Nodes] ],
		      [Redfunc,Num_reducers,[R_todo,[]] ]
		     ],		     
		     Files, 
		     Nodes);
						    

	%% On making a map request the task_tracker responds immediately 
	%% by sending a copy of the list of input files. This is updated 
	%% on the jobtracker
	{files,Node,Input_files} ->
	    [[ITodo,IDone], Inter, Result] = Files,
	    jtracker(Reg_name, Time, Functions, 
		     [
		      [[[Node, Ifile] || Ifile <- Input_files]++ITodo, IDone],
		      Inter,
		      Result], Nodes);	       
	
		
	%% mapper_result_success, notification for successful completion
	%% of the mapping of an input file to intermediate files
	%% @Node,  node on which mapper ran
	%% @Filename, name of the input file
	%% @Inter_files, list of intermediate files
	%% 
	%% Update the status and loop over.
	{mapper_result_success, Node, Filename, Inter_files} ->	    

	    io:format("~nmapper_result_success on ~p",[Filename]),

	    [ [InpTodo,InpDone],[IntTodo,IntDone],Result ] = Files,
	    
	    Temp = [ [Node,F] || F <- Inter_files],
	    
	    [ _ , [ _, _,[R_todo,[]] ]] = Functions,
	    
	    [Fname,_] = string:tokens(Filename,"."),

	    lists:map( fun([N, Id]) ->			       
			       [Num] = io_lib:format("~p",[Id]),
			       Oname = Fname ++ "_" ++ Num ++ ".int",
			       rpc:sbcast([N],Reg_name,
					  {reduce_files, [[N,Oname]], Id})
		       end,
		       R_todo ),

	    jtracker(Reg_name, Time, Functions,
		     [ [InpTodo -- [[Node,Filename]], 
			InpDone++[[Node,Filename]]],
		       [IntTodo ++ Temp, IntDone], Result
		     ], Nodes);       	    


	%% On completion of map job on a node
	%% @Node, node on which mapper_completed
	%%
	%% On completion of mapper, check if there any unassigned reducer jobs
	%% which might be taken on.
	{mapper_complete, Node} ->

	    io:format("~nMapping complete on node: ~p ",[Node]),	    
	    %% Once mapper is complete on one node, it can be 
	    %% used as a reducer	    
	    [[Mapfunc, Node_status],RED] =
				Functions,
	    New_node_status =  ((Node_status -- [Node,active]) 
				++ [Node,complete]),
	    
	    
	    [ [InpTodo,_],_, _ ] = Files,
	    [ _ , [ _, _,[R_todo,[]] ]] = Functions,
	    
	    case
		%%lists:filter(fun([_,S]) -> S=:=active end, New_node_status) 
		InpTodo
	    of
		[] ->
		    io:format("~nAll map jobs are complete"),
		    io:format("~nRequesting completion of reducers~n"),
		    
		    lists:map(fun([N,Id]) ->
				      rpc:sbcast([N],Reg_name,
						 {reduce_return, Id})
			      end,
			      R_todo);		    
		_ ->
		    ok
	    end,
	    
	    jtracker(Reg_name, Time,
		     [[Mapfunc, New_node_status], RED],
		     Files,
		     Nodes
		     );
		    
	%% Last arg is [Done,Bad] which is not used here.
	{reduce_return_filename, Id, Node, Filename, [_,_]} ->
	    io:format("~nReducer returned file(id:~p) : ~p on ~p~n",
		      [Id,Filename,Node]),
	    jtracker(Reg_name, Time, Functions, Files, Nodes);
	
	%% This is just to know the status of the jobtracker without 
	%% interrupting its operations.
	{jobtracker_status} ->
	    io:format("~n Functions : ~p",[Functions]),
	    io:format("~n Files : ~p",[Files]),
	    io:format("~n Nodes : ~p~n",[Nodes]),
	    jtracker(Reg_name, Time, Functions, Files, Nodes);
			
	  
	%% flush for all weird messages 
	Any ->
	    io:format("~n jTracker received a WEIRD message ~p~n",[Any]),
	    jtracker(Reg_name, Time, Functions, Files, Nodes)
    end.
	        	    	          

% Default submissions with preser Mapfunc, Redfunc, numbers and ids
% Mapfunc = mapper()
% N_mappers = 5
% Redfunc = reducer()
% N_reducers = 3
% Data_ids = "*.{map,red,fin}"
submit_job() ->

    jTracker ! {job_request, 
		fun(X)->                 %%  Mapfunc 
			H = list_to_integer(X) div 10,
			{X,H}
		end,    
		"Data/*.map",            %%  Inp_pattern
		3,                       %%  Num_mappers		
		fun(X)->{X} end,         %%  Redfunc
		3                        %%  Num_reducers
	       }.


%len([]) ->
%    0;
%len([_|T]) ->
%    1+len(T).


roundrobin(List, Id) ->
    roundrobin(List, List, Id, []).

roundrobin(List, [H|T], [Hi|Ti], Acc)->
    roundrobin(List, T, Ti,  [[H,Hi] | Acc]);
roundrobin(List, [], Id, Acc) ->
    roundrobin(List, List, Id, Acc);
roundrobin(_, _, [], Acc) ->
    lists:reverse(Acc).
    
    

    
    
    



