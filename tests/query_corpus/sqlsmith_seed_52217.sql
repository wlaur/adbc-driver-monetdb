-- Deterministic SQLsmith query corpus for ADBC integration replay.
-- SQLsmith commit: 16aad4e262f09b286ae891926c990df266feedfd
-- Generation seed: 52217; original statements: 100
-- Original corpus SHA-256: 11f1c3d70cb871558c50e3d5cb2f2279278bbe801f722376059eab19ecad864e
-- Reduced to structurally varied accepted queries plus one expected server rejection.

-- query: 8; expected: ok
select
  ref_0.order_id as c0,
  ref_0.created_at as c1,
  ref_0.note as c2,
  case when ref_0.note is not NULL then ref_0.created_at else ref_0.created_at end
     as c3
from
  sys.review_sqlsmith_orders as ref_0
where true;

-- query: 18; expected: ok
select
  ref_0.customer_id as c0
from
  sys.review_sqlsmith_orders as ref_0
where EXISTS (
  select
      ref_1.active as c0
    from
      sys.review_sqlsmith_customers as ref_1
        inner join sys.review_sqlsmith_customers as ref_2
        on (ref_0.created_at is not NULL),
      lateral (select
            ref_0.note as c0,
            (select amount from sys.review_sqlsmith_orders limit 1 offset 97)
               as c1
          from
            sys.review_sqlsmith_customers as ref_3
              right join sys.review_sqlsmith_orders as ref_4
                left join sys.review_sqlsmith_orders as ref_5
                on (ref_4.order_id = ref_5.order_id )
              on ((ref_0.note is not NULL)
                  and ((true)
                    and (ref_3.name is not NULL)))
          where (ref_2.name is not NULL)
            or ((((ref_0.created_at is NULL)
                  or (ref_4.amount is not NULL))
                and (ref_3.active is NULL))
              and (false))
          limit 94) as subq_0
    where (EXISTS (
        select
            ref_6.order_id as c0
          from
            sys.review_sqlsmith_orders as ref_6
          where ref_1.active is NULL))
      or (true)
    limit 106)
limit 94;

-- query: 23; expected: ok
select
  ref_0.amount as c0,
  ref_0.customer_id as c1,
  ref_0.note as c2,
  ref_0.customer_id as c3
from
  sys.review_sqlsmith_orders as ref_0
where (EXISTS (
    select
        ref_1.active as c0,
        ref_0.customer_id as c1,
        ref_1.name as c2,
        ref_1.active as c3
      from
        sys.review_sqlsmith_customers as ref_1
      where ref_0.order_id is NULL))
  and (false)
limit 90;

-- query: 28; expected: ok
select
  ref_0.name as c0,
  ref_0.active as c1,
  ref_0.name as c2
from
  sys.review_sqlsmith_customers as ref_0
where false;

-- query: 29; expected: ok
select
  ref_0.amount as c0,
  ref_0.note as c1,
  ref_0.order_id as c2
from
  sys.review_sqlsmith_orders as ref_0
where true;

-- query: 30; expected: ok
select
  subq_2.c2 as c0
from
  (select
          21 as c0,
          subq_1.c1 as c1,
          subq_1.c0 as c2
        from
          (select
                ref_1.name as c0,
                ref_1.customer_id as c1,
                ref_1.customer_id as c2,
                ref_1.active as c3
              from
                sys.review_sqlsmith_orders as ref_0
                  right join sys.review_sqlsmith_customers as ref_1
                  on (EXISTS (
                      select
                          ref_2.created_at as c0,
                          ref_2.customer_id as c1
                        from
                          sys.review_sqlsmith_orders as ref_2,
                          lateral (select
                                ref_3.order_id as c0,
                                ref_1.customer_id as c1
                              from
                                sys.review_sqlsmith_orders as ref_3
                              where true) as subq_0
                        where true
                        limit 104))
              where false
              limit 147) as subq_1
        where (subq_1.c2 is NULL)
          or (subq_1.c0 is NULL)
        limit 115) as subq_2
    left join sys.review_sqlsmith_customers as ref_4
    on ((((ref_4.active is NULL)
            or ((((subq_2.c2 is NULL)
                  and ((subq_2.c1 is not NULL)
                    or (true)))
                and (false))
              or (true)))
          or (EXISTS (
            select
                ref_4.active as c0
              from
                sys.review_sqlsmith_orders as ref_5
              where ((((ref_4.name is not NULL)
                      and ((ref_4.name is not NULL)
                        or (false)))
                    or (true))
                  and (ref_5.note is not NULL))
                or (EXISTS (
                  select
                      ref_4.joined as c0
                    from
                      sys.review_sqlsmith_orders as ref_6
                    where false
                    limit 44))
              limit 104)))
        and (ref_4.joined is NULL))
where true
limit 67;

-- query: 43; expected: ok
select
  ref_0.note as c0,
  ref_0.customer_id as c1
from
  sys.review_sqlsmith_orders as ref_0
where ((select created_at from sys.review_sqlsmith_orders limit 1 offset 4)
       is not NULL)
  or (ref_0.created_at is NULL)
limit 51;

-- query: 44; expected: ok
select
  (select customer_id from sys.review_sqlsmith_customers limit 1 offset 32)
     as c0,
  subq_0.c5 as c1,
  subq_0.c7 as c2,
  subq_0.c7 as c3,
  subq_0.c2 as c4,
  (select name from sys.review_sqlsmith_customers limit 1 offset 3)
     as c5,
  subq_0.c6 as c6,
  subq_0.c7 as c7
from
  (select
        ref_1.note as c0,
        ref_0.name as c1,
        (select order_id from sys.review_sqlsmith_orders limit 1 offset 5)
           as c2,
        ref_1.amount as c3,
        ref_0.name as c4,
        ref_1.created_at as c5,
        (select active from sys.review_sqlsmith_customers limit 1 offset 2)
           as c6,
        ref_0.active as c7,
        (select order_id from sys.review_sqlsmith_orders limit 1 offset 4)
           as c8
      from
        sys.review_sqlsmith_customers as ref_0
          left join sys.review_sqlsmith_orders as ref_1
          on (ref_1.order_id is not NULL)
      where false
      limit 135) as subq_0
where (subq_0.c0 is not NULL)
  and (subq_0.c3 is not NULL)
limit 106;

-- query: 48; expected: ok
select
  case when (((((((((true)
                      or ((true)
                        or (EXISTS (
                          select
                              (select created_at from sys.review_sqlsmith_orders limit 1 offset 3)
                                 as c0,
                              subq_0.c0 as c1
                            from
                              sys.review_sqlsmith_customers as ref_2
                            where true))))
                    and (((true)
                        or ((true)
                          or ((((false)
                                or (true))
                              and ((true)
                                and (EXISTS (
                                  select
                                      (select customer_id from sys.review_sqlsmith_customers limit 1 offset 42)
                                         as c0
                                    from
                                      sys.review_sqlsmith_customers as ref_3
                                    where true
                                    limit 62))))
                            or (true))))
                      or (true)))
                  and ((((true)
                        and ((true)
                          or (EXISTS (
                            select
                                subq_0.c0 as c0,
                                ref_4.name as c1,
                                subq_0.c0 as c2,
                                61 as c3,
                                subq_0.c0 as c4,
                                subq_0.c0 as c5
                              from
                                sys.review_sqlsmith_customers as ref_4
                              where false
                              limit 144))))
                      or ((subq_0.c0 is NULL)
                        and (true)))
                    or ((((true)
                          or ((subq_0.c0 is NULL)
                            and (subq_0.c0 is NULL)))
                        and ((EXISTS (
                            select
                                9 as c0,
                                ref_5.name as c1,
                                subq_0.c0 as c2,
                                ref_5.name as c3,
                                ref_5.name as c4
                              from
                                sys.review_sqlsmith_customers as ref_5
                              where (((((true)
                                        or (EXISTS (
                                          select
                                              ref_6.order_id as c0,
                                              ref_5.active as c1,
                                              subq_0.c0 as c2,
                                              ref_5.active as c3,
                                              (select order_id from sys.review_sqlsmith_orders limit 1 offset 80)
                                                 as c4,
                                              ref_6.customer_id as c5,
                                              ref_5.customer_id as c6,
                                              subq_0.c0 as c7
                                            from
                                              sys.review_sqlsmith_orders as ref_6
                                            where true
                                            limit 119)))
                                      and (true))
                                    or (EXISTS (
                                      select
                                          subq_1.c2 as c0,
                                          (select amount from sys.review_sqlsmith_orders limit 1 offset 4)
                                             as c1,
                                          (select note from sys.review_sqlsmith_orders limit 1 offset 2)
                                             as c2,
                                          ref_7.active as c3,
                                          subq_0.c0 as c4
                                        from
                                          sys.review_sqlsmith_customers as ref_7,
                                          lateral (select
                                                subq_0.c0 as c0,
                                                ref_7.customer_id as c1,
                                                ref_7.name as c2,
                                                subq_0.c0 as c3,
                                                subq_0.c0 as c4,
                                                ref_8.customer_id as c5
                                              from
                                                sys.review_sqlsmith_orders as ref_8
                                              where ((true)
                                                  and (true))
                                                or (true)) as subq_1
                                        where (EXISTS (
                                            select distinct
                                                ref_7.customer_id as c0,
                                                subq_0.c0 as c1
                                              from
                                                sys.review_sqlsmith_orders as ref_9
                                              where subq_0.c0 is not NULL))
                                          or (true)
                                        limit 32)))
                                  and (false))
                                and (subq_0.c0 is not NULL)
                              limit 91))
                          and (((((subq_0.c0 is not NULL)
                                  or (subq_0.c0 is NULL))
                                and (false))
                              or (EXISTS (
                                select
                                    subq_0.c0 as c0,
                                    ref_10.joined as c1
                                  from
                                    sys.review_sqlsmith_customers as ref_10
                                  where (false)
                                    or (false))))
                            or (EXISTS (
                              select
                                  subq_0.c0 as c0,
                                  subq_0.c0 as c1,
                                  ref_11.order_id as c2
                                from
                                  sys.review_sqlsmith_orders as ref_11
                                where false)))))
                      and (EXISTS (
                        select
                            ref_12.joined as c0,
                            ref_12.name as c1,
                            ref_12.name as c2,
                            ref_12.joined as c3,
                            ref_12.name as c4,
                            ref_12.customer_id as c5,
                            ref_12.name as c6,
                            19 as c7,
                            ref_12.joined as c8,
                            subq_0.c0 as c9,
                            80 as c10,
                            subq_0.c0 as c11,
                            subq_0.c0 as c12,
                            58 as c13,
                            subq_0.c0 as c14,
                            ref_12.customer_id as c15,
                            ref_12.customer_id as c16
                          from
                            sys.review_sqlsmith_customers as ref_12
                          where subq_0.c0 is NULL)))))
                and ((false)
                  or (false)))
              or (EXISTS (
                select
                    ref_13.customer_id as c0,
                    24 as c1,
                    ref_13.amount as c2,
                    ref_13.created_at as c3,
                    subq_0.c0 as c4,
                    ref_13.order_id as c5,
                    ref_13.created_at as c6,
                    subq_0.c0 as c7,
                    ref_13.order_id as c8,
                    subq_0.c0 as c9,
                    ref_13.note as c10,
                    subq_0.c0 as c11,
                    (select created_at from sys.review_sqlsmith_orders limit 1 offset 32)
                       as c12
                  from
                    sys.review_sqlsmith_orders as ref_13
                  where ref_13.created_at is NULL
                  limit 128)))
            or (subq_0.c0 is not NULL))
          or ((false)
            and ((((38 is NULL)
                  or (false))
                and (EXISTS (
                  select
                      ref_14.active as c0,
                      subq_0.c0 as c1,
                      subq_0.c0 as c2,
                      ref_14.customer_id as c3,
                      subq_0.c0 as c4
                    from
                      sys.review_sqlsmith_customers as ref_14,
                      lateral (select
                            (select joined from sys.review_sqlsmith_customers limit 1 offset 5)
                               as c0,
                            subq_0.c0 as c1,
                            subq_0.c0 as c2
                          from
                            sys.review_sqlsmith_orders as ref_15
                          where false) as subq_2
                    where false)))
              or ((EXISTS (
                  select
                      33 as c0,
                      subq_0.c0 as c1,
                      ref_16.name as c2,
                      subq_0.c0 as c3,
                      72 as c4,
                      subq_0.c0 as c5,
                      subq_0.c0 as c6,
                      subq_0.c0 as c7,
                      ref_16.active as c8,
                      ref_16.name as c9,
                      ref_16.customer_id as c10,
                      ref_16.active as c11,
                      subq_0.c0 as c12,
                      subq_0.c0 as c13,
                      ref_16.active as c14,
                      subq_0.c0 as c15,
                      ref_16.active as c16,
                      subq_0.c0 as c17,
                      subq_0.c0 as c18,
                      ref_16.joined as c19,
                      ref_16.customer_id as c20,
                      subq_0.c0 as c21,
                      subq_0.c0 as c22
                    from
                      sys.review_sqlsmith_customers as ref_16
                    where ref_16.customer_id is NULL
                    limit 138))
                and ((false)
                  and (subq_0.c0 is NULL))))))
        or (EXISTS (
          select
              ref_17.created_at as c0,
              subq_0.c0 as c1,
              ref_17.created_at as c2,
              ref_17.order_id as c3,
              ref_17.created_at as c4
            from
              sys.review_sqlsmith_orders as ref_17
            where ref_17.order_id is NULL)))
      or (((((((false)
                  and ((false)
                    and ((((((select name from sys.review_sqlsmith_customers limit 1 offset 2)
                                 is not NULL)
                            and (EXISTS (
                              select
                                  (select name from sys.review_sqlsmith_customers limit 1 offset 3)
                                     as c0,
                                  ref_18.name as c1,
                                  subq_0.c0 as c2,
                                  ref_18.name as c3,
                                  subq_0.c0 as c4,
                                  subq_0.c0 as c5,
                                  subq_0.c0 as c6,
                                  ref_18.active as c7,
                                  ref_18.customer_id as c8
                                from
                                  sys.review_sqlsmith_customers as ref_18
                                where ((ref_18.joined is not NULL)
                                    and (subq_0.c0 is not NULL))
                                  or (true)
                                limit 111)))
                          and (subq_0.c0 is NULL))
                        or ((true)
                          or (((EXISTS (
                                select
                                    subq_0.c0 as c0,
                                    subq_0.c0 as c1,
                                    subq_3.c4 as c2,
                                    ref_19.note as c3,
                                    subq_4.c1 as c4,
                                    ref_19.customer_id as c5,
                                    subq_0.c0 as c6,
                                    subq_4.c2 as c7,
                                    ref_19.customer_id as c8,
                                    ref_19.order_id as c9,
                                    subq_0.c0 as c10,
                                    subq_0.c0 as c11,
                                    ref_19.order_id as c12,
                                    subq_3.c2 as c13,
                                    ref_19.order_id as c14,
                                    (select customer_id from sys.review_sqlsmith_customers limit 1 offset 99)
                                       as c15,
                                    subq_3.c4 as c16,
                                    subq_0.c0 as c17,
                                    subq_0.c0 as c18,
                                    subq_0.c0 as c19,
                                    subq_3.c4 as c20
                                  from
                                    sys.review_sqlsmith_orders as ref_19,
                                    lateral (select
                                          28 as c0,
                                          ref_19.customer_id as c1,
                                          (select order_id from sys.review_sqlsmith_orders limit 1 offset 1)
                                             as c2,
                                          ref_20.active as c3,
                                          ref_19.amount as c4,
                                          subq_0.c0 as c5
                                        from
                                          sys.review_sqlsmith_customers as ref_20
                                        where (EXISTS (
                                            select
                                                ref_21.created_at as c0,
                                                subq_0.c0 as c1,
                                                ref_19.order_id as c2,
                                                subq_0.c0 as c3
                                              from
                                                sys.review_sqlsmith_orders as ref_21
                                              where subq_0.c0 is not NULL
                                              limit 61))
                                          or (((false)
                                              and ((subq_0.c0 is not NULL)
                                                and (((false)
                                                    and (true))
                                                  or ((false)
                                                    and ((ref_20.joined is NULL)
                                                      or (false))))))
                                            or (true))) as subq_3,
                                    lateral (select
                                          subq_3.c5 as c0,
                                          subq_3.c3 as c1,
                                          subq_0.c0 as c2,
                                          subq_0.c0 as c3
                                        from
                                          sys.review_sqlsmith_orders as ref_22
                                        where (false)
                                          or ((((false)
                                                and ((true)
                                                  or (false)))
                                              or (false))
                                            or (ref_22.amount is NULL))
                                        limit 10) as subq_4
                                  where EXISTS (
                                    select
                                        subq_3.c3 as c0,
                                        subq_3.c4 as c1,
                                        subq_0.c0 as c2,
                                        (select name from sys.review_sqlsmith_customers limit 1 offset 3)
                                           as c3,
                                        subq_0.c0 as c4,
                                        subq_4.c1 as c5,
                                        subq_4.c2 as c6,
                                        subq_0.c0 as c7,
                                        subq_4.c3 as c8,
                                        subq_4.c1 as c9,
                                        subq_4.c3 as c10,
                                        subq_3.c4 as c11,
                                        subq_0.c0 as c12
                                      from
                                        sys.review_sqlsmith_orders as ref_23
                                      where EXISTS (
                                        select
                                            ref_19.created_at as c0,
                                            ref_24.joined as c1,
                                            subq_5.c0 as c2
                                          from
                                            sys.review_sqlsmith_customers as ref_24,
                                            lateral (select
                                                  subq_3.c5 as c0
                                                from
                                                  sys.review_sqlsmith_customers as ref_25
                                                where true
                                                limit 80) as subq_5
                                          where (subq_0.c0 is not NULL)
                                            or (false)
                                          limit 142))
                                  limit 155))
                              and ((true)
                                and ((true)
                                  and (((false)
                                      and ((EXISTS (
                                          select
                                              ref_26.note as c0,
                                              (select customer_id from sys.review_sqlsmith_orders limit 1 offset 5)
                                                 as c1,
                                              42 as c2,
                                              subq_0.c0 as c3
                                            from
                                              sys.review_sqlsmith_orders as ref_26
                                            where (((true)
                                                  or (ref_26.note is NULL))
                                                or (subq_0.c0 is NULL))
                                              or ((ref_26.amount is NULL)
                                                and (true))
                                            limit 13))
                                        or (subq_0.c0 is NULL)))
                                    and (EXISTS (
                                      select
                                          ref_27.note as c0,
                                          (select note from sys.review_sqlsmith_orders limit 1 offset 71)
                                             as c1
                                        from
                                          sys.review_sqlsmith_orders as ref_27
                                        where (select customer_id from sys.review_sqlsmith_customers limit 1 offset 3)
                                             is not NULL
                                        limit 80))))))
                            and ((((((((true)
                                          or (false))
                                        or (false))
                                      and (false))
                                    or (((((((true)
                                                and (false))
                                              and (subq_0.c0 is NULL))
                                            and (false))
                                          and (true))
                                        and (false))
                                      and (EXISTS (
                                        select
                                            ref_28.active as c0,
                                            (select note from sys.review_sqlsmith_orders limit 1 offset 5)
                                               as c1,
                                            ref_28.name as c2
                                          from
                                            sys.review_sqlsmith_customers as ref_28
                                          where (ref_28.name is not NULL)
                                            or (false)))))
                                  and ((true)
                                    and (true)))
                                and ((((EXISTS (
                                        select
                                            ref_29.customer_id as c0
                                          from
                                            sys.review_sqlsmith_customers as ref_29,
                                            lateral (select
                                                  ref_29.joined as c0,
                                                  100 as c1,
                                                  ref_30.created_at as c2,
                                                  ref_30.customer_id as c3,
                                                  ref_30.amount as c4,
                                                  ref_30.created_at as c5,
                                                  ref_29.name as c6,
                                                  ref_30.customer_id as c7,
                                                  subq_0.c0 as c8
                                                from
                                                  sys.review_sqlsmith_orders as ref_30
                                                where true) as subq_6
                                          where (((false)
                                                or (false))
                                              and (true))
                                            or (subq_6.c3 is not NULL)
                                          limit 98))
                                      and (subq_0.c0 is not NULL))
                                    and (subq_0.c0 is NULL))
                                  or (((false)
                                      or ((EXISTS (
                                          select
                                              subq_7.c0 as c0,
                                              subq_7.c0 as c1,
                                              ref_31.created_at as c2,
                                              ref_31.created_at as c3,
                                              subq_7.c0 as c4,
                                              subq_7.c0 as c5,
                                              subq_7.c0 as c6,
                                              subq_0.c0 as c7,
                                              subq_0.c0 as c8,
                                              subq_7.c0 as c9,
                                              subq_7.c0 as c10
                                            from
                                              sys.review_sqlsmith_orders as ref_31,
                                              lateral (select
                                                    ref_31.note as c0
                                                  from
                                                    sys.review_sqlsmith_orders as ref_32
                                                  where true
                                                  limit 28) as subq_7
                                            where (((ref_31.amount is not NULL)
                                                  or (subq_0.c0 is not NULL))
                                                or (EXISTS (
                                                  select
                                                      subq_8.c4 as c0,
                                                      subq_8.c0 as c1,
                                                      ref_31.amount as c2,
                                                      ref_31.note as c3,
                                                      subq_0.c0 as c4
                                                    from
                                                      sys.review_sqlsmith_orders as ref_33,
                                                      lateral (select
                                                            subq_7.c0 as c0,
                                                            subq_0.c0 as c1,
                                                            (select customer_id from sys.review_sqlsmith_customers limit 1 offset 1)
                                                               as c2,
                                                            ref_33.order_id as c3,
                                                            ref_34.customer_id as c4
                                                          from
                                                            sys.review_sqlsmith_customers as ref_34
                                                          where true) as subq_8
                                                    where true
                                                    limit 119)))
                                              or (false)
                                            limit 97))
                                        and (true)))
                                    and (subq_0.c0 is not NULL))))
                              or ((EXISTS (
                                  select
                                      subq_0.c0 as c0,
                                      ref_35.active as c1,
                                      ref_35.joined as c2,
                                      ref_35.joined as c3,
                                      ref_35.name as c4,
                                      ref_35.joined as c5,
                                      subq_0.c0 as c6,
                                      ref_35.joined as c7,
                                      ref_35.name as c8,
                                      53 as c9,
                                      subq_0.c0 as c10,
                                      (select joined from sys.review_sqlsmith_customers limit 1 offset 2)
                                         as c11,
                                      subq_0.c0 as c12
                                    from
                                      sys.review_sqlsmith_customers as ref_35
                                    where false))
                                or (false))))))
                      or (subq_0.c0 is not NULL))))
                or (true))
              and (((subq_0.c0 is not NULL)
                  and (subq_0.c0 is NULL))
                and (((true)
                    or ((subq_0.c0 is not NULL)
                      or (subq_0.c0 is NULL)))
                  and ((((subq_0.c0 is not NULL)
                        and (subq_0.c0 is NULL))
                      or (true))
                    or (((true)
                        or ((true)
                          and ((subq_0.c0 is NULL)
                            and (subq_0.c0 is NULL))))
                      or ((false)
                        or ((false)
                          or (false))))))))
            or ((true)
              and (false)))
          and (EXISTS (
            select
                subq_10.c1 as c0
              from
                sys.review_sqlsmith_customers as ref_36,
                lateral (select
                      subq_0.c0 as c0,
                      subq_0.c0 as c1,
                      21 as c2,
                      subq_0.c0 as c3,
                      ref_37.joined as c4,
                      ref_37.joined as c5,
                      59 as c6,
                      ref_37.name as c7,
                      ref_36.joined as c8,
                      ref_36.active as c9
                    from
                      sys.review_sqlsmith_customers as ref_37
                    where ref_36.name is not NULL) as subq_9,
                lateral (select
                      ref_36.active as c0,
                      (select joined from sys.review_sqlsmith_customers limit 1 offset 6)
                         as c1,
                      subq_0.c0 as c2,
                      subq_0.c0 as c3,
                      subq_0.c0 as c4,
                      subq_0.c0 as c5,
                      ref_38.name as c6
                    from
                      sys.review_sqlsmith_customers as ref_38
                    where ref_38.name is not NULL
                    limit 61) as subq_10
              where subq_10.c3 is NULL
              limit 174)))
        or (false)) then 33 else case when ((((((true)
                  or ((subq_0.c0 is not NULL)
                    and (subq_0.c0 is NULL)))
                or (subq_0.c0 is not NULL))
              or (subq_0.c0 is not NULL))
            and ((false)
              or (((((EXISTS (
                        select
                            subq_14.c7 as c0,
                            subq_14.c7 as c1,
                            subq_14.c1 as c2,
                            ref_39.amount as c3
                          from
                            sys.review_sqlsmith_orders as ref_39,
                            lateral (select
                                  ref_40.joined as c0,
                                  subq_12.c1 as c1,
                                  subq_11.c3 as c2,
                                  subq_12.c0 as c3,
                                  ref_39.created_at as c4,
                                  subq_0.c0 as c5,
                                  subq_12.c0 as c6,
                                  ref_39.customer_id as c7
                                from
                                  sys.review_sqlsmith_customers as ref_40,
                                  lateral (select
                                        ref_39.note as c0,
                                        ref_40.active as c1,
                                        ref_39.created_at as c2,
                                        ref_41.active as c3,
                                        ref_41.name as c4,
                                        1 as c5,
                                        ref_39.order_id as c6,
                                        subq_0.c0 as c7
                                      from
                                        sys.review_sqlsmith_customers as ref_41
                                      where (false)
                                        or (subq_0.c0 is NULL)) as subq_11,
                                  lateral (select
                                        ref_39.note as c0,
                                        subq_0.c0 as c1,
                                        subq_0.c0 as c2
                                      from
                                        sys.review_sqlsmith_orders as ref_42
                                      where false
                                      limit 95) as subq_12,
                                  lateral (select
                                        subq_12.c1 as c0,
                                        subq_0.c0 as c1,
                                        ref_40.customer_id as c2
                                      from
                                        sys.review_sqlsmith_orders as ref_43
                                      where ref_40.joined is NULL
                                      limit 96) as subq_13
                                where false) as subq_14
                          where (subq_0.c0 is NULL)
                            or ((true)
                              or (true))
                          limit 116))
                      or (true))
                    or (true))
                  or (false))
                and (EXISTS (
                  select
                      subq_0.c0 as c0
                    from
                      sys.review_sqlsmith_orders as ref_44
                    where (ref_44.customer_id is not NULL)
                      or (EXISTS (
                        select
                            subq_0.c0 as c0,
                            subq_0.c0 as c1,
                            ref_44.created_at as c2,
                            subq_0.c0 as c3,
                            ref_44.amount as c4,
                            ref_45.created_at as c5
                          from
                            sys.review_sqlsmith_orders as ref_45
                          where ((true)
                              and ((EXISTS (
                                  select
                                      ref_46.customer_id as c0,
                                      ref_44.created_at as c1,
                                      ref_44.order_id as c2,
                                      subq_0.c0 as c3,
                                      ref_44.amount as c4,
                                      ref_44.order_id as c5,
                                      subq_0.c0 as c6,
                                      ref_45.customer_id as c7,
                                      subq_0.c0 as c8,
                                      ref_44.order_id as c9,
                                      ref_46.created_at as c10,
                                      subq_0.c0 as c11,
                                      ref_46.created_at as c12,
                                      ref_44.order_id as c13
                                    from
                                      sys.review_sqlsmith_orders as ref_46
                                    where false
                                    limit 61))
                                or (true)))
                            and (true)
                          limit 85))
                    limit 139)))))
          and (((((true)
                  and (false))
                and ((true)
                  and (true)))
              or ((true)
                and (false)))
            or ((false)
              and ((true)
                and ((false)
                  and (subq_0.c0 is not NULL))))))
        and (subq_0.c0 is not NULL) then cast(coalesce(case when (select amount from sys.review_sqlsmith_orders limit 1 offset 1)
               is not NULL then case when (false)
              or (true) then 76 else 96 end
             else 79 end
          ,
        19) as int) else case when false then case when (subq_0.c0 is not NULL)
            and (((true)
                or (subq_0.c0 is NULL))
              and ((EXISTS (
                  select
                      subq_0.c0 as c0,
                      ref_47.joined as c1,
                      subq_0.c0 as c2,
                      ref_47.joined as c3,
                      ref_47.customer_id as c4,
                      14 as c5,
                      (select created_at from sys.review_sqlsmith_orders limit 1 offset 5)
                         as c6,
                      subq_0.c0 as c7,
                      ref_47.name as c8,
                      ref_47.active as c9
                    from
                      sys.review_sqlsmith_customers as ref_47
                    where true
                    limit 135))
                or ((EXISTS (
                    select
                        subq_0.c0 as c0,
                        ref_48.note as c1,
                        ref_48.created_at as c2,
                        ref_48.note as c3
                      from
                        sys.review_sqlsmith_orders as ref_48
                      where (((false)
                            and ((true)
                              and (true)))
                          or ((((EXISTS (
                                  select
                                      subq_15.c8 as c0,
                                      subq_0.c0 as c1,
                                      ref_49.note as c2,
                                      (select note from sys.review_sqlsmith_orders limit 1 offset 6)
                                         as c3,
                                      ref_48.customer_id as c4,
                                      1 as c5
                                    from
                                      sys.review_sqlsmith_orders as ref_49,
                                      lateral (select
                                            (select customer_id from sys.review_sqlsmith_orders limit 1 offset 3)
                                               as c0,
                                            ref_48.note as c1,
                                            subq_0.c0 as c2,
                                            subq_0.c0 as c3,
                                            ref_49.order_id as c4,
                                            ref_48.order_id as c5,
                                            ref_48.order_id as c6,
                                            85 as c7,
                                            ref_50.order_id as c8,
                                            ref_50.created_at as c9,
                                            subq_0.c0 as c10,
                                            (select amount from sys.review_sqlsmith_orders limit 1 offset 28)
                                               as c11,
                                            subq_0.c0 as c12
                                          from
                                            sys.review_sqlsmith_orders as ref_50
                                          where EXISTS (
                                            select
                                                ref_51.order_id as c0
                                              from
                                                sys.review_sqlsmith_orders as ref_51
                                              where true
                                              limit 111)
                                          limit 154) as subq_15
                                    where (false)
                                      or (false)
                                    limit 100))
                                and ((29 is not NULL)
                                  or (true)))
                              or (((false)
                                  or (true))
                                or (false)))
                            or (ref_48.created_at is NULL)))
                        and (true)))
                  or (subq_0.c0 is NULL)))) then cast(coalesce(66,
            78) as int) else 32 end
           else 97 end
         end
       end
     as c0
from
  (select
        ref_0.order_id as c0
      from
        sys.review_sqlsmith_orders as ref_0
          left join sys.review_sqlsmith_customers as ref_1
          on (ref_0.note = ref_1.name )
      where ref_0.order_id is NULL
      limit 158) as subq_0
where (EXISTS (
    select
        ref_52.customer_id as c0,
        ref_52.amount as c1,
        subq_0.c0 as c2,
        cast(nullif(subq_0.c0,
          cast(null as bigint)) as bigint) as c3,
        subq_0.c0 as c4,
        ref_52.order_id as c5,
        ref_52.customer_id as c6,
        ref_52.amount as c7,
        ref_52.amount as c8,
        subq_0.c0 as c9,
        91 as c10,
        subq_0.c0 as c11,
        ref_52.created_at as c12,
        subq_0.c0 as c13,
        (select customer_id from sys.review_sqlsmith_customers limit 1 offset 45)
           as c14,
        ref_52.order_id as c15,
        subq_0.c0 as c16
      from
        sys.review_sqlsmith_orders as ref_52
      where EXISTS (
        select
            ref_53.customer_id as c0,
            ref_53.amount as c1,
            ref_53.amount as c2,
            ref_53.customer_id as c3
          from
            sys.review_sqlsmith_orders as ref_53
              right join sys.review_sqlsmith_orders as ref_54
              on ((((false)
                      and (ref_52.amount is not NULL))
                    and (EXISTS (
                      select
                          ref_52.customer_id as c0,
                          ref_54.note as c1
                        from
                          sys.review_sqlsmith_customers as ref_55
                        where (ref_52.customer_id is not NULL)
                          and (ref_52.amount is NULL)
                        limit 73)))
                  and ((true)
                    and ((EXISTS (
                        select
                            ref_54.created_at as c0,
                            ref_52.order_id as c1,
                            ref_53.note as c2,
                            85 as c3,
                            ref_52.created_at as c4,
                            ref_52.customer_id as c5,
                            (select joined from sys.review_sqlsmith_customers limit 1 offset 1)
                               as c6,
                            ref_54.customer_id as c7,
                            ref_53.created_at as c8,
                            ref_53.note as c9
                          from
                            sys.review_sqlsmith_customers as ref_56
                          where true))
                      or (((subq_0.c0 is NULL)
                          or (false))
                        or (((false)
                            or ((true)
                              and (true)))
                          or (true))))))
          where ((((false)
                  and (ref_54.amount is NULL))
                and (((false)
                    or (((EXISTS (
                          select
                              ref_52.amount as c0,
                              ref_52.created_at as c1,
                              ref_57.joined as c2,
                              ref_57.name as c3
                            from
                              sys.review_sqlsmith_customers as ref_57
                            where false
                            limit 109))
                        and (false))
                      and ((true)
                        or (EXISTS (
                          select
                              subq_0.c0 as c0,
                              subq_0.c0 as c1,
                              ref_58.note as c2
                            from
                              sys.review_sqlsmith_orders as ref_58
                            where false
                            limit 118)))))
                  or (false)))
              or (subq_0.c0 is not NULL))
            or ((true)
              or ((true)
                or (false))))
      limit 156))
  or (((false)
      or (false))
    or ((subq_0.c0 is NULL)
      or (subq_0.c0 is NULL)))
limit 106;

-- query: 62; expected: ok
select
  (select amount from sys.review_sqlsmith_orders limit 1 offset 1)
     as c0
from
  (select
        ref_1.joined as c0,
        ref_2.customer_id as c1,
        ref_2.amount as c2,
        ref_0.customer_id as c3,
        ref_0.joined as c4
      from
        sys.review_sqlsmith_customers as ref_0
          inner join sys.review_sqlsmith_customers as ref_1
            right join sys.review_sqlsmith_orders as ref_2
            on (ref_2.customer_id is NULL)
          on (ref_0.name = ref_1.name ),
        lateral (select
              ref_0.name as c0,
              ref_1.joined as c1,
              ref_2.created_at as c2,
              ref_1.customer_id as c3,
              ref_1.name as c4
            from
              sys.review_sqlsmith_orders as ref_3
            where ref_3.amount is not NULL) as subq_0
      where (false)
        or (ref_2.amount is NULL)) as subq_1
where 86 is NULL;

-- query: 68; expected: ok
select
  ref_0.note as c0,
  ref_0.customer_id as c1,
  ref_0.order_id as c2
from
  sys.review_sqlsmith_orders as ref_0
where (ref_0.created_at is NULL)
  or ((ref_0.note is not NULL)
    and (ref_0.order_id is not NULL))
limit 104;

-- query: 76; expected: ok
select
  (select name from sys.review_sqlsmith_customers limit 1 offset 4)
     as c0,
  subq_1.c1 as c1
from
  (select
        subq_0.c0 as c0,
        89 as c1,
        subq_0.c4 as c2,
        subq_0.c1 as c3,
        cast(nullif(subq_0.c4,
          subq_0.c4) as boolean) as c4
      from
        (select
              ref_0.customer_id as c0,
              ref_0.active as c1,
              ref_0.name as c2,
              ref_0.name as c3,
              ref_0.active as c4
            from
              sys.review_sqlsmith_customers as ref_0
            where EXISTS (
              select
                  ref_1.active as c0,
                  ref_0.name as c1,
                  (select active from sys.review_sqlsmith_customers limit 1 offset 15)
                     as c2,
                  ref_0.active as c3,
                  25 as c4
                from
                  sys.review_sqlsmith_customers as ref_1
                where (true)
                  and ((ref_1.customer_id is not NULL)
                    or (EXISTS (
                      select
                          29 as c0,
                          ref_2.customer_id as c1,
                          ref_1.name as c2,
                          ref_2.name as c3,
                          ref_1.joined as c4,
                          ref_1.active as c5,
                          ref_1.active as c6,
                          ref_1.active as c7,
                          ref_1.customer_id as c8,
                          ref_1.active as c9,
                          ref_1.name as c10
                        from
                          sys.review_sqlsmith_customers as ref_2
                        where false
                        limit 130)))
                limit 160)
            limit 77) as subq_0
      where subq_0.c3 is not NULL
      limit 37) as subq_1
where true
limit 48;

-- query: 77; expected: ok
select
  subq_0.c0 as c0
from
  (select
        ref_0.amount as c0
      from
        sys.review_sqlsmith_orders as ref_0
          right join sys.review_sqlsmith_customers as ref_1
          on (false)
      where (ref_1.joined is not NULL)
        and (false)) as subq_0
where subq_0.c0 is NULL
limit 117;

-- query: 79; expected: ok
select
  ref_0.joined as c0,
  ref_0.name as c1,
  (select active from sys.review_sqlsmith_customers limit 1 offset 5)
     as c2,
  ref_0.customer_id as c3,
  ref_0.customer_id as c4,
  ref_0.name as c5
from
  sys.review_sqlsmith_customers as ref_0
where ref_0.name is NULL
limit 143;

-- query: 81; expected: ok
select
  subq_0.c0 as c0,
  subq_0.c1 as c1,
  subq_0.c1 as c2,
  (select customer_id from sys.review_sqlsmith_orders limit 1 offset 4)
     as c3,
  subq_0.c1 as c4,
  cast(coalesce(subq_0.c0,
    subq_0.c0) as int) as c5,
  subq_0.c0 as c6,
  subq_0.c0 as c7,
  (select created_at from sys.review_sqlsmith_orders limit 1 offset 3)
     as c8
from
  (select
        ref_0.customer_id as c0,
        ref_1.note as c1,
        ref_2.order_id as c2
      from
        sys.review_sqlsmith_customers as ref_0
          inner join sys.review_sqlsmith_orders as ref_1
            left join sys.review_sqlsmith_orders as ref_2
            on (ref_2.note is NULL)
          on (false)
      where true
      limit 159) as subq_0
where subq_0.c2 is NULL;

-- query: 85; expected: programming_error 42000
WITH
jennifer_0 AS (select
    ref_0.name as c0,
    (select note from sys.review_sqlsmith_orders limit 1 offset 5)
       as c1
  from
    sys.review_sqlsmith_customers as ref_0
  where ref_0.active is not NULL
  limit 51)
select
    subq_0.c2 as c0,
    subq_0.c0 as c1,
    subq_0.c3 as c2,
    case when EXISTS (
        select
            subq_0.c2 as c0,
            ref_3.c1 as c1,
            ref_3.c1 as c2,
            subq_0.c1 as c3,
            ref_2.customer_id as c4,
            ref_3.c0 as c5,
            subq_0.c2 as c6,
            ref_2.joined as c7,
            ref_2.joined as c8
          from
            sys.review_sqlsmith_customers as ref_2
              inner join jennifer_0 as ref_3
              on (ref_2.name = ref_3.c0 )
          where false
          limit 95) then subq_0.c3 else subq_0.c2 end
       as c3,
    subq_0.c3 as c4
  from
    (select
          ref_1.joined as c0,
          ref_1.name as c1,
          ref_1.active as c2,
          ref_1.active as c3
        from
          sys.review_sqlsmith_customers as ref_1
        where (((ref_1.active is NULL)
              and (false))
            and ((((true)
                  or (ref_1.joined is not NULL))
                or (true))
              or ((ref_1.joined is NULL)
                or ((select name from sys.review_sqlsmith_customers limit 1 offset 2)
                     is not NULL))))
          and (true)) as subq_0
  where (subq_0.c0 is NULL)
    or ((((((subq_0.c0 is NULL)
              or (EXISTS (
                select
                    subq_0.c3 as c0,
                    ref_4.customer_id as c1,
                    subq_0.c3 as c2,
                    ref_4.created_at as c3,
                    ref_4.order_id as c4,
                    (select created_at from sys.review_sqlsmith_orders limit 1 offset 1)
                       as c5,
                    ref_4.note as c6,
                    ref_4.amount as c7,
                    subq_0.c2 as c8,
                    (select active from sys.review_sqlsmith_customers limit 1 offset 4)
                       as c9,
                    subq_0.c3 as c10
                  from
                    sys.review_sqlsmith_orders as ref_4
                  where subq_0.c2 is NULL
                  limit 122)))
            and ((subq_0.c2 is not NULL)
              and (EXISTS (
                select
                    ref_5.c0 as c0,
                    ref_5.c0 as c1,
                    ref_5.c0 as c2,
                    90 as c3,
                    ref_5.c1 as c4,
                    ref_5.c0 as c5,
                    subq_0.c3 as c6,
                    ref_5.c0 as c7,
                    98 as c8,
                    ref_5.c0 as c9,
                    subq_0.c1 as c10,
                    subq_0.c1 as c11,
                    ref_5.c0 as c12,
                    86 as c13,
                    subq_0.c0 as c14,
                    ref_5.c0 as c15,
                    ref_5.c0 as c16,
                    ref_5.c0 as c17,
                    (select name from sys.review_sqlsmith_customers limit 1 offset 1)
                       as c18,
                    (select active from sys.review_sqlsmith_customers limit 1 offset 37)
                       as c19,
                    subq_0.c1 as c20,
                    ref_5.c1 as c21,
                    subq_0.c0 as c22
                  from
                    jennifer_0 as ref_5
                  where ref_5.c0 is not NULL
                  limit 65))))
          or (((43 is NULL)
              or (((EXISTS (
                    select
                        subq_0.c1 as c0,
                        ref_6.c0 as c1,
                        subq_1.c2 as c2
                      from
                        jennifer_0 as ref_6,
                        lateral (select
                              ref_7.customer_id as c0,
                              (select active from sys.review_sqlsmith_customers limit 1 offset 5)
                                 as c1,
                              58 as c2,
                              subq_0.c3 as c3,
                              subq_0.c3 as c4,
                              ref_7.active as c5
                            from
                              sys.review_sqlsmith_customers as ref_7
                            where (ref_7.active is not NULL)
                              or (false)) as subq_1
                      where true))
                  and (subq_0.c3 is not NULL))
                or (subq_0.c3 is not NULL)))
            or ((99 is not NULL)
              or (true))))
        and (EXISTS (
          select
              ref_8.active as c0,
              ref_8.customer_id as c1,
              (select name from sys.review_sqlsmith_customers limit 1 offset 5)
                 as c2,
              55 as c3,
              ref_8.active as c4,
              subq_0.c2 as c5
            from
              sys.review_sqlsmith_customers as ref_8
            where ((false)
                or ((false)
                  or ((false)
                    and (true))))
              or (EXISTS (
                select
                    24 as c0,
                    subq_0.c0 as c1,
                    ref_8.joined as c2,
                    ref_9.customer_id as c3,
                    ref_8.joined as c4,
                    ref_9.customer_id as c5,
                    68 as c6,
                    ref_8.active as c7,
                    ref_9.customer_id as c8,
                    ref_9.customer_id as c9,
                    ref_9.customer_id as c10,
                    subq_0.c2 as c11
                  from
                    sys.review_sqlsmith_customers as ref_9
                  where ref_9.active is NULL)))))
      or ((subq_0.c3 is NULL)
        or (false)))
  limit 95
;

-- query: 95; expected: ok
select
  ref_0.joined as c0,
  ref_0.name as c1,
  ref_0.joined as c2
from
  sys.review_sqlsmith_customers as ref_0
where true
limit 136;
