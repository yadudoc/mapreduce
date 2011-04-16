-module(reducer).
-export([
	 reducer/5,
	 readinputs/2,
	 testfun/1
	 ]).


%% The main reducer thread.
%% 
%% 1. On receiving a reduce_files request
%%     1. apply readinputs on the Files
%%     2. get the sorted_Results and apply Reducer function
%%     3. update status
%% 2.  die -> die
%% 3. reduce_return Id
%%     1. Write the result to file
%%     2. return the result filename to tracker
%% NOTE: THE LIST OF FILES PASSED TO THE reducer MUST BE OF THE FORM
%%   [ [node,file1], [node, file2] ...]
reducer(Redfunc, Acc, Tracker, Id, [Done, Bad]) ->
%%    io:format("~nAcc  ~p~n Files ~p~n",[Acc,[Done,Bad]]),
    receive
	{reduce_files, Files, Id} ->	    
	    [Sorted_inputs ,[New_good,New_bad]] = 
		readinputs(Files, [Done,Bad]),  
%%	    io:format("~nreducer, sorted results = ~p",[Sorted_inputs]),
	    io:format("~nReducing file(s) = ~p",
		      [Files]),	    
	    reducer(Redfunc,
		    Redfunc(Sorted_inputs),
		    Tracker,
		    Id,
		    [ (Done -- New_good) ++ New_good ,
		      (Bad  -- New_bad ) ++ New_bad ]
		   );
	
	{die} ->
	    io:format("~n reducer dying...~n");
	
	{reduce_return, Id} ->
	    [N] = io_lib:format("~p",[Id]),
	    Out = "MapReduce_result_" ++ N ++ ".res",
	    fileio:writelines(write,Out,Acc),
	    Tracker ! {reduce_return_filename, Id, Out, [Done,Bad]},
	    io:format("~nReducer writing to file ~p~n Acc ~p ~n",
		      [Out,Acc])	    
    end.


%% From a given list of intermediate files passed thrrough @Files
%% remove the ones which are already processed.
%% The local files are listed first for being processed first.
readinputs( Files, [Done, Bad] ) ->   
    
    %% remove all files which are already processed
    Temp = [ [N,F] || [N,F] <- Files,
		      Done =:= Done -- [F] ],    

    %% Place files present on the local node at the beginning
    Temp2 = lists:append(
	      lists:filter(fun([N,_])->
				   N =:= node()
			   end,
			   Temp),
	      lists:filter(fun([N,_])->
				   N =/= node()
			   end,
		 Temp)
	     ),    
    readinputs( Temp2, [], [Done,Bad]).
		 

readinputs([[Node,Filename]|T], Acc, [Done, Bad] )->

    case [Filename] -- Done of
	%% File not present in Done list
	[Filename] ->
	    case Node =:= node() of
		false ->
%%		    io:format("~n Accessing file ~p on ~p",[Filename,Node]),
		    {Status, Contents} = 
			rpc:call(Node, fileio, readline, [Filename]);
		true ->
%%		    io:format("~n Accessing local-file ~p",[Filename]),
		    {Status, Contents} =
			fileio:readline(Filename)
	    end,
	    
	    case Status of
		error ->
		    readinputs(T,Acc,[Done, Bad++[Filename]]);
		ok ->
		    readinputs(T,
			       lists:append(Acc,Contents),
			       [Done ++ [Filename], Bad]
			       )
	    end;
	
	%% File is already processed
	[] ->
		   readinputs(T,[Done,Bad])	    	        
    end;
	     
readinputs([], Acc, [Done,Bad]) ->
    [ lists:sort(Acc), [Done,Bad] ].
    		





%% testfun is a sample reduce function to test out the 
%% working.
%% given a list of key,value pairs testfun returns a list
%% such that for all the k,v pairs passed to it, the ones
%% with the same keys have their values summed up
testfun([])->
    [];

testfun([H|T]) ->    
    [K,V] = [ {list_to_integer(X)} || X <- string:tokens(H,",") ],
    testfun(T, K, V,[]).

testfun([H|T], PreKey, PreVal, Acc)->
    [K,V] = [ {list_to_integer(X)} || X <- string:tokens(H,",") ],
    
    case K =:= PreKey of
	false ->
	    testfun(T, K, V, [ [PreKey,PreVal] | Acc ]);
	true ->
	    {V1} = PreVal,
	    {V2} = V,
	    testfun(T, K, {V1+V2}, Acc)
    end;

testfun([], PreKey, PreVal, Acc) ->
    lists:reverse ([ [PreKey,PreVal]|Acc]).

