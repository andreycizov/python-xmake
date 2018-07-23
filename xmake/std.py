from xmake.dsl import Fun, Match, Case, Err

assert_single = Fun(
    lambda items: Match(
        items,
        lambda m: [
            Case(
                m.len == 1,
                m[0]
            ),
            Case(
                True,
                Err('Wrong number of items: %s', m.len)
            ),
        ]
    )
)

assert_not_none = Fun(
    lambda x: Match(
        x,
        lambda m: [
            Case(
                m == None,
                Err('%s is None', m)
            ),
            Case(
                True,
                m
            )
        ]
    )
)
