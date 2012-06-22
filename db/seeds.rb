# This file should contain all the record creation needed to seed the database with its default values.
# The data can then be loaded with the rake db:seed (or created alongside the db with db:setup).
#
# Examples:
#
#   cities = City.create([{ name: 'Chicago' }, { name: 'Copenhagen' }])
#   Mayor.create(name: 'Emanuel', city: cities.first)

require 'csv'

TWITTER_ACCOUNTS=<<EOF
screen_name,password,consumer_key,consumer_secret,access_token_key,access_token_secret
a69358,daviddavid,E9tajkCz2pfhiJMnkcckg,29FDYTgBT0sI41YWtkOFiI3FRQ9xf81naU1R0H10,501202165-nwamXGGCGNcSw5rYMUE3NR9MD11zjrC7hncxspt4,0KxFh89Gxo3XbMMA2sPkT708tCz5HCqXlWtnUpm9w
a69904,daviddavid,dbbBD48RF4B817rFGR8Efw,gZvCtU4rWPQUngTdKCwejs0mZkn68p2lqDnafZzF4,501205456-q43NOfdfMxfW3fu2tYtvreWvJIegibbtomtAqE7t,jlrDpk8t5BV4wOxDZFjdwSY85tJssWo7h3lpFT7Vq5c
a72909,daviddavid,yiKQWqezhjRntIZTup6vg,WK9wbXnhVqTTURYeeahTB4HBnM3N12kWkAwxBXa64,501236274-Bw4URXOro1mCgf1T62yAvi8rMgRHmShVkXt1dSyq,42tnamL2nogVSeSJNRmCkZclCFY2Cnw1hYN3vlVEAEA
a73422,daviddavid,LBQYM9F8pHbDvtCOlPig,6ICFLhECeMgtdJyThfojPf9LB9D2n1JcXaolDyQBzA,501242173-2a4TZAFDzN8SORDHNcUNl0F1iBpiKp6CEwlFncAz,TDIBZjvD1HPsdSunwPuNiPOjxpvL2txRo26Ni38c
a67584,daviddavid,yVf1YqKv5ZlMaSZaeLgA,jQ3nbYfHNb39qUDiH8gKxJlAgR3P2g0dR6uCGxlXmQ4,501244901-TfaC1RyuPaKgcIxoT54pLzTqUtd1OOFfa67MCqc1,omCmSlTMGaTpoD8bHOdv8jvb3RHFxlPcAPldXIXPDVA
a74353,daviddavid,lSh7GXMSXOM9YV5Pph7phw,qitXTUUQSlC30qcTi1BwhzJBuVxP2rtpCpXLzI6A,501252974-o9X9XNb2mHVMFDaTHImR6zx65FYCZEqRl0KcBPKo,bKHfhcbvC5cZkPalp5ZsNVPTOPoA0vkfPliShcE
a74565,daviddavid,e0Nbyurm2yv9LVMv7gAQ,VMvHG19bgE8fhaAtLsXVXX8VI7hz7dI2CyXMteia8,501255994-BRVzqYy7XCAmo9n9UsjfEs8vQ5XmHOcGk3pWX7Bb,ijqRIX1cXzWpRotaLSx4t1GnD3f4BMwEdOPjoYKfE
a83174,daviddavid,xlDvayIylmVIzerCEQ1fQg,AWFLnCVF89UZPKSz9eNNX1nUI5mepS17FVCvdsF8I,501366890-kxwdeuBVa26KoJ5c4td88j03Ht7sY75VVMEpcY3p,dPLhPGy7n9UUIaONH6AY8IylZ1Awy7rZpveEbA5s
a83296,daviddavid,C8Pk2aOeGhs1q19BjXXtiA,KqBpoIqRENaFhskAnObD8mOTTOhvQjD7Bd0uGaQyyjE,501368503-dmJNa5W9WQz2fGqTHit3wz5UyNpK8xKuu9hK71ic,xOvuPH48s0fW5xKTzqrIpep9CAzYyKxCXh0v1zxc55U
a83620,daviddavid,0MuqpiyUpGrSDhwVCf5lw,vuYBA15ntgfi0h9tDjxDQ7dNzWZN9tkSU8qnWPXZnQA,501371103-efiA7aUu85JuaSgCPKt9GP0Nq1v37V7dbeKXEzUw,99PY9vSQAv86ndpYIZRYWElQs9OXrGDHs0S4XtiQHN4
a83824,daviddavid,Fbx57TrC1Gpf1dfsp88w,H55MopFVX5xqB0WIdk7VqtrUQV9iqBix2x8tIE8eoc,501372835-k7xDZbHJhLAS3W7LuDWwRbxKXu7ZXFropcemBa8p,AG5LsJfe20jqlofJ9d9WyBtFqc6rbOh0OkJW46g
966Alina,daviddavid,DU8ola2dUj5EqrMH5Scomw,dSHBRCemQtjvfFkkVqhsXUtbG2ookCz0RL8vOUzJPps,501386679-BB8idv6r3WgfYtE5yY4L2jJibb0wYYEQWx8D0h0r,0IiItu70jrK80NhkZqZwTaLb5FpnWLJRSg2YEdtk
a85739,daviddavid,GQACBzbZ8yqPf3uMPlZADQ,2TKmjDEc9YYa4GvExQbO9PEVhVtfwd7RARaWl8af10E,501389891-8BEC27QZoe6xVLdbfHFgFKyODkmaQPKPd8T3jnQ3,oZU7CLi8asWyfRftgevh2alvYZIS8m3DGMuMS5Uaxe4
a87266,daviddavid,jp0Ai4x6yA9enpcru9g,ffI9THQWTcgOk9DXY7UCMwk26rbZLodglwrnj5IA9E,501407657-FcvoT1I6v9lSTgs67T54be0vRatsWUXZ0H3LQukL,EbAgYCL3p2JZ4JM12v9K7NOj7xcvFGZHbYW5fMVF9Bc
a87410,daviddavid,HwCyKmerdifprEOTTKMTWQ,Kspt1e7JNTeLrvcyr4RwFMFnJEBDS3GeKEpPsWvSyMI,501409278-NPdThmPWDo6YIOxXf6rXz92WsnFLeVoyzdteXYDJ,KWyamn0AVcoHHxgMNzyk8yoXdavXs1Wi2XOayGmg
caecreak,daviddavid,CdLApkH9xv5WcTKuxiRIA,0cYzowJWRiFfHExOCA7CXEfHVAmlgbwmtDI6D9yUzpU,501418995-97Xxntw9yKxHLMgWveClyHhDcFNAh9tVZPolfcXY,PD3gHqW7QY56PFzdbtFateWNgwc85SpuAdPShX3snE
prioriaj,daviddavid,zJ7V0fHcgv3kkCOx8JQ9Lw,b2FcyFZqktuhk3bncOfTM7LfLyBkG7h8Ki9jk7licQ,501420139-YDOBZ1BWHwIy9iLBblZ03qUYAyLQfy8SRuQcMpjd,3nNC1VyiVTY95nhDWN4FTAm2VlWhqJ9QPTVb2LKVXw
mgiut,daviddavid,zcCxN7IqGI7M6WqZN8AkwA,Wo4bzwg2zkWYWZ2Ad09jEb7XxOgV1cwwBB9YkLDUuFg,501421320-iXYLPAMxlZfPTxCfd6oUWkvosOT52MaHC0vctPQT,cqPqKlc0JgFFrCAs9NPH919fXzbItyet6GSRWhqAQ
UoboM,daviddavid,o1GOLy7w1QXxRyH1hQE3g,ko5SEyfA1YZ4zWrdU7zZqZcoiLHhluvA2IqaALM5E,501422463-0YdD1TjNqfQ8Oe21f8oLEzNWDkgtW3sFmwwUIw,zk9PuzbRYtwbLKkcATvuWJgyEiNIxtrkDwD9zioGo
pronioch,daviddavid,mI5ZtEL7C2G9QPJ2HEww,YxvwsctNfF6Adxzr70WM1Aph1NBXaSdTBg8snpasaaM,501423726-Qgiu01PPFYRYK4hTmgCr5xIfjtLXbgsmmupo6S8H,TJmRQVpuWn5hO9tCSjxL2G870SB1C7KHiUbXO3xgpc
ralphsmitch,----,XH1ZmuuHyHfIKc1y3avKVg,V5Yl421q1LK6o6bgBmN05hyLULT8S1ofdTmn6ZHfTY,500908387-h1dfQ4lN3VK5jp9ryFKsIqnTZwOo5l5zktTn9KjO,nzukR8kuRsYGoF1zcp1nhxSjMKdeJcDkXVu7YxCQTg
wallacebowman,----,1OqPqOBkCeg57pRju4IEpg,duoNfBfMc1bhbUGZIpdi20Rhekm5OaLIl1LUjJzknI,500906161-gmHa57rUI9sOCDRNWFm5kyRTsznNMcNn9EIqY3u9,RSrvPz3AWVDZ8d8R2q9wkAD5jAJ2xoLngu8Zx1daf0
EOF

twitter_accounts = CSV.parse(TWITTER_ACCOUNTS, :headers => true)
twitter_accounts.each do |account|
  ETL::TwitterProcessor.create(:client_options => {
                                 :oauth_token => account["access_token_key"],
                                 :oauth_token_secret => account["access_token_secret"],
                                 :consumer_key => account["consumer_key"],
                                 :consumer_secret => account["consumer_secret"]
                               })
end
