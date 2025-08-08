MUTATE_RULES = [
{
    id: 0,
    condition: 'Filter after GROUP BY',
    transformation: 'PUSH filter through GROUP BY'
},

{
    id: 1,
    condition: 'LIMIT after LEFT JOIN',
    transformation: 'Propogate LIMIT through LEFT JOIN'
},


{
    id:3,
    condition: 'Expression is reducible',
    transformation: 'Reduce expression in FILTER'
}
# // {
# //     id:4
# //     condition:
# //     transformation:
# // }
]
