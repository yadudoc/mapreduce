% P31
% Determine whether a given number is prime
-module(is_prime).
-export([check/1, check/2]).

% returns true if the number is prime else returns false
% divide and check by numbers until sqrt of N


check(debug, N) ->
    check(N, 2, erlang:trunc(math:sqrt(N)) + 1, debug).

check(N) ->
    check(N, 2, erlang:trunc(math:sqrt(N)) + 1, none).


check(_, Max, Max, _) ->
    true;
check(N, I, Max, Type) ->
    if 
	N rem I =:= 0 , Type =:= debug ->
	    {false,I};
	N rem I =:= 0 ->
	    false;
	true ->
	    check(N, I+1, Max, Type)
    end.
	
