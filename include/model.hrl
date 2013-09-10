-include("db.hrl").
-export([new/0, new/1, create/0, create/1, find/1, find_all/0, find_all/1, find_all/2,
  update/1, update/2, delete/1, save/1, find_or_new/1, find_or_create/1, find_in_batches/2, find_in_batches/3]).

-ifndef(MAP).
-define(MAP(Record), Record).
-endif.

new() -> #?MODULE{}.

new(Fields) ->
  set_values(Fields, new()).

set_values({Field, Value}, Record) ->
  setelement(field_index(Field), Record, Value);
set_values([], Record) -> Record;
set_values([Pair | Rest], Record) ->
  set_values(Rest, set_values(Pair, Record)).

save(Record = #?MODULE{id = undefined}) ->
  create(Record);
save(Record) ->
  update(Record).

find_or_new(Criteria) ->
  case find(Criteria) of
    undefined -> new(Criteria);
    Record -> Record
  end.

find_or_create(Criteria) ->
  case find(Criteria) of
    undefined -> create(new(Criteria));
    Record -> Record
  end.

create() -> create(#?MODULE{}).

create(Record = #?MODULE{}) ->
  Now = {datetime, calendar:universal_time()},
  RecordToInsert = Record#?MODULE{created_at = Now, updated_at = Now},
  Id = db:insert(insert_query(RecordToInsert)),
  RecordToInsert#?MODULE{id = Id}.

find(Id) when is_number(Id) ->
  find({id, Id});

find(Criteria) ->
  find_with_cache(Criteria).

-ifdef(CACHE).

find_with_cache(Criteria) ->
      cache:get({?MODULE, Criteria}, fun() ->
        case find_without_cache(Criteria) of
          undefined -> undefined;
          Record -> {{?MODULE, {id, Record#?MODULE.id}}, Record}
        end
  end).

-else.

find_with_cache(Criteria) ->
  find_without_cache(Criteria).

-endif.

find_without_cache(Criteria) ->
  case db:select_one(iolist_to_binary(select_query(Criteria, []))) of
    undefined -> undefined;
    Row ->
      Record = list_to_tuple([?MODULE | Row]),
      ?MAP(Record)
  end.

find_all() -> find_all([], []).

find_all(Criteria) -> find_all(Criteria, []).

find_all(Criteria, Options) ->
  Rows = db:select(iolist_to_binary(select_query(Criteria, Options))),
  lists:map(fun(Row) ->
    Record = list_to_tuple([?MODULE | Row]),
    ?MAP(Record)
  end, Rows).

find_in_batches(Criteria, Fun) ->
  find_in_batches(Criteria, 0, fun(Batch, S) -> Fun(Batch), S end, undefined).

find_in_batches(Criteria, Fun, State) ->
  find_in_batches(Criteria, 0, Fun, State).

find_in_batches(Criteria, From, Fun, State) ->
  case find_all([{id, '>', From} | Criteria], [{order_by, id}, {limit, 1000}]) of
    [] -> ok;
    Batch ->
      NewState = Fun(Batch, State),
      Last = lists:last(Batch),
      find_in_batches(Criteria, Last#?MODULE.id, Fun, NewState)
  end.

update(Record = #?MODULE{}) ->
  Now = {datetime, calendar:universal_time()},
  RecordToUpdate = Record#?MODULE{updated_at = Now},
  db:update(update_query(RecordToUpdate)),
  RecordToUpdate.

update(Fields, Record) ->
  UpdatedRecord = set_values(Fields, Record),
  update(UpdatedRecord).

delete(Record = #?MODULE{}) ->
  1 = db:update(delete_query(Record)),
  ok.

insert_query(Record) ->
  insert_fields(<<"INSERT INTO ", ?TABLE_NAME, "(">>, Record, record_info(fields, ?MODULE)).

insert_fields(Query, Record, [id | Rest]) ->
  insert_fields(Query, Record, Rest);
insert_fields(Query, Record, [Field]) ->
  FieldBin = atom_to_binary(Field, latin1),
  insert_values(<<Query/binary, "`", FieldBin/binary, "`) VALUES (">>, Record, 3, record_info(size, ?MODULE));
insert_fields(Query, Record, [Field | Rest]) ->
  FieldBin = atom_to_binary(Field, latin1),
  insert_fields(<<Query/binary, "`", FieldBin/binary, "`, ">>, Record, Rest).

insert_values(Query, Record, Index, Count) when Index =:= Count ->
  ValueBin = list_to_binary(mysql:encode(element(Index, Record))),
  <<Query/binary, ValueBin/binary, ")">>;
insert_values(Query, Record, Index, Count) ->
  ValueBin = list_to_binary(mysql:encode(element(Index, Record))),
  insert_values(<<Query/binary, ValueBin/binary, ", ">>, Record, Index + 1, Count).

select_query(Criteria, Options) ->
  ["SELECT " | select_fields(Criteria, Options, record_info(fields, ?MODULE))].

select_fields(Criteria, Options, [Field]) ->
  ["`", atom_to_list(Field), "` FROM ", ?TABLE_NAME | select_criteria(Criteria, Options)];
select_fields(Criteria, Options, [Field | Rest]) ->
  ["`", atom_to_list(Field), "`, " | select_fields(Criteria, Options, Rest)].

select_criteria([], Options) -> select_options(Options);
select_criteria(Criteria, Options) ->
  [" WHERE ", select_criteria(Criteria) | select_options(Options)].

select_criteria({Field, in, Values}) ->
  [atom_to_list(Field), " IN (", value_list(Values), ")"];

select_criteria({C1, 'or', C2}) ->
  ["(", select_criteria(C1), " OR ", select_criteria(C2), ")"];

select_criteria({Field, '<', Value}) ->
  [atom_to_list(Field), " < ", mysql:encode(Value)];

select_criteria({Field, '<=', Value}) ->
  [atom_to_list(Field), " <= ", mysql:encode(Value)];

select_criteria({Field, '>', Value}) ->
  [atom_to_list(Field), " > ", mysql:encode(Value)];

select_criteria({Field, '>=', Value}) ->
  [atom_to_list(Field), " >= ", mysql:encode(Value)];

select_criteria({Field, null}) ->
  [atom_to_list(Field), " IS NULL"];

select_criteria({Field, Value}) ->
  [atom_to_list(Field), " = ", mysql:encode(Value)];

select_criteria([Filter]) ->
  select_criteria(Filter);

select_criteria([Filter | Rest]) ->
  [select_criteria(Filter), " AND " | select_criteria(Rest)].

select_options([]) -> [];
select_options([{order_by, Field} | Rest]) ->
  [" ORDER BY ", atom_to_list(Field) | select_options(Rest)];
select_options([{limit, Limit} | Rest]) ->
  [" LIMIT ", integer_to_list(Limit) | select_options(Rest)].

update_query(Record) ->
  ["UPDATE ", ?TABLE_NAME, " SET " | update_fields(Record, 2, record_info(fields, ?MODULE))].

update_fields(Record, Index, [id | Rest]) ->
  update_fields(Record, Index + 1, Rest);
update_fields(Record, Index, [Field]) ->
  [atom_to_list(Field), " = ", mysql:encode(element(Index, Record)), " WHERE id = ", mysql:encode(Record#?MODULE.id)];
update_fields(Record, Index, [Field | Rest]) ->
  [atom_to_list(Field), " = ", mysql:encode(element(Index, Record)), ", " | update_fields(Record, Index + 1, Rest)].


delete_query(Record) ->
  ["DELETE FROM ", ?TABLE_NAME, " WHERE id = ", mysql:encode(Record#?MODULE.id)].

value_list([]) -> [];
value_list([Value]) ->
  [mysql:encode(Value)];
value_list([Value | Rest]) ->
  [mysql:encode(Value), ", " | value_list(Rest)].

field_index(Field) ->
  field_index(Field, record_info(fields, ?MODULE), 2).

field_index(Field, [Field | _], N) -> N;
field_index(Field, [_ | Rest], N) -> field_index(Field, Rest, N + 1).
