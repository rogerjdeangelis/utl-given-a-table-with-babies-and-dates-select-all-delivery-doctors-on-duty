# utl-given-a-table-with-babies-and-dates-select-all-delivery-doctors-on-duty
Given a table with babies and dates select all delivery doctors on duty
    %let pgm=utl-given-a-table-with-babies-and-dates-select-all-delivery-doctors-on-duty;

    Given a table with babies and dates select all delivery doctors on duty;

      Four Solutions
          1. SAS SQl
          2. WPS SQL
          3. WPS PROC R
          4. WPS PROC PYTHON  (Had to trim the join variables)


    github
    https://tinyurl.com/4pcfbywm
    https://github.com/rogerjdeangelis/utl-given-a-table-with-babies-and-dates-select-all-delivery-doctors-on-duty

    StackOverflow
    https://tinyurl.com/47nkx46p
    https://stackoverflow.com/questions/31686730/is-it-possible-to-merge-two-datasets-where-a-variables-value-in-the-first-is-us?rq=1

    /*                   _
    (_)_ __  _ __  _   _| |_ ___
    | | `_ \| `_ \| | | | __/ __|
    | | | | | |_) | |_| | |_\__ \
    |_|_| |_| .__/ \__,_|\__|___/
            |_|
    */

    data baby;
      input baby $ day $;
    cards4;
    Jake DAY1
    Sonny DAY4
    North DAY5
    Apple DAY6
    ;;;;
    run;quit;

    data doctors(drop=day1-day6);
     length delivery_day $4;
     retain delivery_day;
    input  doc $ day1 day2 day3 day4 day5 day6;
    array days day: ;
    do over days;
      delivery_day = vname(days);
      if days ne 0 then output;
    end;
    datalines;
    Jones 1 0 0 1 1 1
    Smith 0 1 1 0 0 0
    Lewis 1 1 1 0 0 0
    ;
    run;quit;

    proc sort data=doctors;
    by delivery_day;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* WORK.BABIES total obs=4 04MAY2023:10:53:43                                                                             */
    /*                                                                                                                        */
    /*  Obs    BABY     DAY                                                                                                   */
    /*                                                                                                                        */
    /*   1     Jake     DAY1                                                                                                  */
    /*   2     Sonny    DAY4                                                                                                  */
    /*   3     North    DAY5                                                                                                  */
    /*   4     Apple    DAY6                                                                                                  */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* I sorted the input just for documentation purposes the solution dooes not reqire a sorted dataset                      */
    /*                                                                                                                        */
    /* WORK.DOCTORS total obs=9 04MAY2023:11:18:07                                                                            */
    /*                                                                                                                        */
    /*         DELIVERY_          |   RULES (Join tables on Birth Days)                                                       */
    /*  Obs       DAY        DOC  |                                                                                           */
    /*                            |                                                                                           */
    /*   1       DAY1       Jones |   Jake Two delivery doctors on duty the day jake was born                                 */
    /*   2       DAY1       Lewis |                                                                                           */
    /*                            |                                                                                           */
    /*   3       DAY2       Smith |   No babies born on these dates so no match on date                                       */
    /*   4       DAY2       Lewis |                                                                                           */
    /*   5       DAY3       Smith |                                                                                           */
    /*   6       DAY3       Lewis |                                                                                           */
    /*                            |                                                                                           */
    /*   7       DAY4       Jones |   Sonny Only one delivery doctor on duty the day Sonny was born                           */
    /*   8       DAY5       Jones |   North Only one delivery doctor on duty the day North was born                           */
    /*   9       DAY6       Jones |   Apple Only one delivery doctor on duty the day Apple was born                           */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*           _               _
      ___  _   _| |_ _ __  _   _| |_
     / _ \| | | | __| `_ \| | | | __|
    | (_) | |_| | |_| |_) | |_| | |_
     \___/ \__,_|\__| .__/ \__,_|\__|
                    |_|
    */

    proc sql;
     create
        table merged as
     select
        babies.*, doctors.*
     from babies, doctors
     where doctors.day(babies.birth_day) = 1; *<--- incorrect;
    quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* Up to 40 obs from last table WORK.WANT total obs=5 04MAY2023:11:30:41                                                  */
    /*                                                                                                                        */
    /* Obs    BABY     DAY      DOC                                                                                           */
    /*                                                                                                                        */
    /*  1     Jake     DAY1    Jones                                                                                          */
    /*  2     Jake     DAY1    Lewis                                                                                          */
    /*  3     Sonny    DAY4    Jones                                                                                          */
    /*  4     North    DAY5    Jones                                                                                          */
    /*  5     Apple    DAY6    Jones                                                                                          */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                         _
     ___  __ _ ___   ___  __ _| |
    / __|/ _` / __| / __|/ _` | |
    \__ \ (_| \__ \ \__ \ (_| | |
    |___/\__,_|___/ |___/\__, |_|
                            |_|
    */

    proc sql;
      create
         table want as
      select
         l.baby
        ,l.day
        ,r.doc
      from
        baby as l left join  doctors as r
      on
       l.day = r.delivery_day
    ;quit;

    /*                              _
    __      ___ __  ___   ___  __ _| |
    \ \ /\ / / `_ \/ __| / __|/ _` | |
     \ V  V /| |_) \__ \ \__ \ (_| | |
      \_/\_/ | .__/|___/ |___/\__, |_|
             |_|                 |_|
    */

    %let _pth=%sysfunc(pathname(work));

    %utl_submit_wps64('

    libname wrk "&_pth";

    proc sql;
      create
         table want as
      select
         l.baby
        ,l.day
        ,r.doc
      from
        wrk.baby as l left join  wrk.doctors as r
      on
       l.day = r.delivery_day
    ;quit;

    proc print;
    run;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /* Obs    BABY     DAY      DOC                                                                                           */
    /*                                                                                                                        */
    /*  1     Jake     DAY1    Jones                                                                                          */
    /*  2     Jake     DAY1    Lewis                                                                                          */
    /*  3     Sonny    DAY4    Jones                                                                                          */
    /*  4     North    DAY5    Jones                                                                                          */
    /*  5     Apple    DAY6    Jones                                                                                          */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*
    __      ___ __  ___   _ __  _ __ ___   ___   _ __
    \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `__|
     \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |
      \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_|
             |_|         |_|
    */

    proc datasets nodetails nolist;
      delete want;;
    run;quit;

    %let _pth=%sysfunc(pathname(work));

    %utl_submit_wps64('

    libname wrk "&_pth";

    proc r;
    export data=wrk.baby    r=baby;
    export data=wrk.doctors r=doctors;
    submit;
    library(sqldf);
    want <- sqldf("
      select
         l.baby
        ,l.day
        ,r.doc
      from
        baby as l left join doctors as r
      on
       l.day = r.delivery_day
    ");
    want;
    ;quit;
    endsubmit;

    import data=want r=want;

    proc print;
    run;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /* Obs    BABY     DAY      DOC                                                                                           */
    /*                                                                                                                        */
    /*  1     Jake     DAY1    Jones                                                                                          */
    /*  2     Jake     DAY1    Lewis                                                                                          */
    /*  3     Sonny    DAY4    Jones                                                                                          */
    /*  4     North    DAY5    Jones                                                                                          */
    /*  5     Apple    DAY6    Jones                                                                                          */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                                _   _
    __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __
    \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \
     \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | |
      \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_|
             |_|         |_|    |___/
    */

    proc datasets nodetails nolist;
      delete want;;
    run;quit;

    %let _pth=%sysfunc(pathname(work));

    %utl_submit_wps64('

    libname wrk "&_pth";

    proc python;

    export data=wrk.baby    python=baby;
    export data=wrk.doctors python=doctors;

    submit;

    from os import path;
    import pandas as pd;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
    mysql = lambda q: sqldf(q, globals());

    print(baby);
    print(doctors);

    baby.info();
    doctors.info();

    want = pdsql("""
         select
            l.baby
           ,l.day
           ,r.doc
         from
           baby as l left join doctors as r
         where
          trim(l.day) = trim(r.delivery_day)
    """);
    print(want);
    endsubmit;

    import data=want python=want;

    proc print;
    run;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  WPS System                                                                                                            */
    /*                                                                                                                        */
    /*    BABY     DAY      DOC                                                                                               */
    /*                                                                                                                        */
    /*    Jake     DAY1    Jones                                                                                              */
    /*    Jake     DAY1    Lewis                                                                                              */
    /*    Sonny    DAY4    Jones                                                                                              */
    /*    North    DAY5    Jones                                                                                              */
    /*    Apple    DAY6    Jones                                                                                              */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
