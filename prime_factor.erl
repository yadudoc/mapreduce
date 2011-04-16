% P35
% Determine the prime factors of a given integers
-module(prime_factor).
-export([pf/1]).


pf(N) ->
    % We take a list of all element in the range ( 2, N/2 )
    List =  for(2, N div 2, [] ) ,
    % We filter the above list based on whether the element
    % is a prime and is a factor of N 
    [X || X <- List,
	  is_prime:check(X),
	  N rem X =:= 0].
		


for(N, N, Acc) ->
    lists:append(Acc,[N]);
for(I, N, Acc) ->
    for(I+1, N, lists:append(Acc,[I])).
