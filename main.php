<?php
// PERFORMANCE REPORT ODVOZENÝ Z OUT-BUCKETU DAKTELA

require_once "vendor/autoload.php";

// načtení konfiguračního souboru
$ds         = DIRECTORY_SEPARATOR;
$dataDir    = getenv("KBC_DATADIR");

// pro případ importu parametrů zadaných JSON kódem v definici PHP aplikace v KBC
$configFile = $dataDir."config.json";
$config     = json_decode(file_get_contents($configFile), true);

// časový rozsah historie pro tvorbu reportu
$reportIntervHistDays = $config['parameters']['reportIntervHistDays'];              // pole má klíče "start" a "end", kde musí být "start" >= "end"

// ==============================================================================================================================================================================================

$tabsIn = ["queues", "queueSessions", "pauseSessions", "activities", "records"];    // vstupní tabulky

$tabsOut = [                                                                        // výstupní tabulky
    "users"     =>  ["date", "iduser", "Q", "QA", "QAP", "QP", "P", "AP", "queueSession", "pauseSession",
                     "talkTime", "idleTime", "activityTime", "callCount", "callCountAnswered" /*, "transactionCount"*/], 
    "sumary"    =>  ["date", "activityTime", "queueSessionTime", "pauseSessionTime", "talkTime", "idleTime", "callCount", "callCountAnswered", "recordsTouched",
                     "recordsDropped", "recordsTimeout", "recordsBusy", "recordsDenied"],
    "events"    =>  ["iduser", "time", "type", "method"]
];         
$tabsOutList = array_keys($tabsOut);
// ==============================================================================================================================================================================================
// časový rozsah reportu

$actualDatestamp = strtotime(date('Y-m-d'));
$reportIntervDates    = [   "start" =>  date('Y-m-d',(strtotime(-$reportIntervHistDays["start"].' day', $actualDatestamp))), 
                            "end"   =>  date('Y-m-d',(strtotime(-$reportIntervHistDays["end"]  .' day', $actualDatestamp)))
                        ];
$reportIntervTimes    = [   "start" =>  $reportIntervDates["start"].' 00:00:00', 
                            "end"   =>  $reportIntervDates["end"]  .' 23:59:59'
                        ];
// ==============================================================================================================================================================================================
// načtení vstupních souborů

foreach ($tabsIn as $file) {
    ${$file} = new Keboola\Csv\CsvFile($dataDir."in".$ds."tables".$ds.$file.".csv");
}
// vytvoření výstupních souborů
foreach ($tabsOutList as $file) {
    ${"out_".$file} = new \Keboola\Csv\CsvFile($dataDir."out".$ds."tables".$ds."out_".$file.".csv");
}
// zápis hlaviček do výstupních souborů
foreach ($tabsOut as $tab => $cols) {
    $colPrf  = "report_performance_".strtolower($tab)."_";  // prefix názvů sloupců ve výstupní tabulce (např. "loginSessions" → "loginsessions_")
    $cols    = preg_filter("/^/", $colPrf, $cols);          // prefixace názvů sloupců ve výstupních tabulkách názvy tabulek kvůli rozlišení v GD (např. "title" → "groups_title")
    ${"out_".$tab} -> writeRow($cols);
}
// ==============================================================================================================================================================================================

$users = $sumary = $events = [];                            // inicializace polí

foreach ($queues as $qNum => $q) {                          // iterace řádků tabulky front
    if ($qNum == 0) {continue;}                             // vynechání hlavičky tabulky
    $q_idqueue = $q[0];
            
    foreach ($queueSessions as $qsNum => $qs) {             // foreach ($queueSessions as $qs)
        if ($qsNum == 0) {continue;}                        // vynechání hlavičky tabulky
        $qs_start_time = $qs[1];
        $qs_end_time   = $qs[2];
        $qs_idqueue    = $qs[4];
        $qs_iduser     = $qs[5];
        
        $qs_start_date = substr($qs_start_time, 0, 10);
        $qs_end_date   = substr($qs_end_time,   0, 10);
        $qs_end_time   = $qs_end_date == $qs_start_date ? $qs_end_time : $qs_start_date.' 23:59:59');

        if ($qs_idqueue != $q_idqueue || $qs_start_time < $reportIntervTimes["start"] || $qs_start_time > $reportIntervTimes["end"]) {
            continue;                                       // queueSession není ze zkoumaného časového rozsahu nebo se netýká dané fronty
        }
        
        // queueSession je ze zkoumaného časového rozsahu
        if (!in_array($qs_start_date, array_keys($users))) {
            $users[$qs_start_date] = [];
        }
        if (!in_array($qs_iduser, array_keys($users[$qs_start_date]))) {
            $user = [                                       // sestavení záznamu do pole uživatelů
                "iduser"            => $qs_iduser,
                "Q"                 => 0,
                "QA"                => 0,
                "QAP"               => 0,
                "QP"                => 0,
                "P"                 => 0,
                "AP"                => 0,
                "queueSession"      => 0,
                "pauseSession"      => 0,
                "talkTime"          => 0,
                "idleTime"          => 0,                            
                "activityTime"      => 0,
                "callCount"         => 0,
                "callCountAnswered" => 0,
                //"transactionCount"  => 0
            ];
            $users [$qs_start_date][$qs_iduser] = $user;    // zápis záznamu do pole uživatelů
            $events[$qs_start_date][$qs_iduser] = [];       // inicializace záznamu do pole událostí
        }
        $users [$qs_start_date][$qs_iduser]["queueSession"] += strtotime($qs_end_time) - strtotime($qs_start_time);
        $event1 = [
            "time"      =>  $qs_start_time,
            "type"      =>  "Q",
            "method"    =>  "+"                   
        ];
        $event2 = [
            "time"      =>  $qs_end_time,
            "type"      =>  "Q",
            "method"    =>  "-"                   
        ];
        $events[$qs_start_date][$qs_iduser][] = $event1; 
        $events[$qs_start_date][$qs_iduser][] = $event2;    // zápis záznamů do pole událostí         
    }
}                                                           // konec iterace front  
// ==============================================================================================================================================================================================
// Get pause sessions + activities + records

foreach ($users as $date => $usersByDay) {
    $sumary[$date] = [
        "activityTime"      => 0,
        "queueSessionTime"  => 0,
        "pauseSessionTime"  => 0,
        "talkTime"          => 0,
        "idleTime"          => 0,
        "callCount"         => 0,
        "callCountAnswered" => 0,
        "recordsTouched"    => 0,
        "recordsDropped"    => 0,
        "recordsTimeout"    => 0,
        "recordsBusy"       => 0,
        "recordsDenied"     => 0
    ];
    
    foreach ($usersByDay as $iduser => $usr) {
        // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
        // Get pause sessions
        foreach ($pauseSessions as $psNum => $ps) {         // foreach ($pauseSessions as $ps) {
            if ($psNum == 0) {continue;}                    // vynechání hlavičky tabulky
            $ps_start_time = $ps[1];
            $ps_end_time   = $ps[2];
            $ps_iduser     = $ps[5];
            
            $ps_start_date = substr($ps_start_time, 0, 10);
            $ps_end_date   = substr($ps_end_time,   0, 10);
            $ps_end_time   = $ps_end_date == $ps_start_date ? $ps_end_time : $ps_start_date.' 23:59:59');

            if ($ps_start_date != $date  ||  $ps_iduser != $iduser) {continue;}
                                                            // pauseSession není ze zkoumaného časového rozsahu nebo se netýká daného uživatele

            // queueSession je ze zkoumaného časového rozsahu
            $users[$ps_start_date][$iduser]["pauseSession"] += strtotime($ps_end_time) - strtotime($ps_start_time);
            $event1 = [
                "time"      =>  $ps_start_time,
                "type"      =>  "P",
                "method"    =>  "+"
            ];
            $event2 = [
                "time"      =>  $ps_end_time,
                "type"      =>  "P",
                "method"    =>  "-"
            ];
            $events[$ps_start_date][$iduser][] = $event1;
            $events[$ps_start_date][$iduser][] = $event2;               
        }                                            
        // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
        // Get activities
        
        foreach ($activities as $aNum => $a) {
            if ($aNum == 0) {continue;}                     // vynechání hlavičky tabulky
            $a_idqueue    = $a[5];
            $a_iduser     = $a[6];
            $a_time       = $a[13];
            $a_type       = $a[10];
            $a_time_open  = $a[15];
            $a_time_close = $a[16];
            $a_item       = $a[19];
            $item         = json_decode($a_item, false);    // dekódováno z JSONu na objekt
            
            $a_date       = substr($a_time, 0, 10);            
            $a_date_open  = substr($a_time_open,  0, 10);
            $a_date_close = substr($a_time_close, 0, 10);
            $a_time_close = $a_date_close == $a_date_open ? $a_time_close : $a_date_open.' 23:59:59');
                                                        
            if ($a_date != $date  ||  $a_iduser != $iduser) {continue;}
                                                            // aktivita není ze zkoumaného časového rozsahu nebo se netýká daného uživatele

            // aktivita je ze zkoumaného časového rozsahu
            if ($a_type == 'CALL' && !empty($item)) {
                $users[$a_date][$iduser]["activityTime"] += strtotime($a_time_close) - strtotime($a_time_open);
                $users[$a_date][$iduser]["talkTime"]     += $item-> duration;      // parsuji duration z objektu $item
                $users[$a_date][$iduser]["callCount"]    += 1;
                if ($item-> answered == "true") {           // parsuji answered z objektu $item
                    $users[$a_date][$iduser]["callCountAnswered"] += 1;
                }
            }
            $event1 = [
                "time"      =>  $a_time_open,
                "type"      =>  "A",
                "method"    =>  "+"                   
            ];
            $event2 = [
                "time"      =>  $a_time_close,
                "type"      =>  "A",
                "method"    =>  "-"                   
            ];
            $events[$a_date][$iduser][] = $event1;
            $events[$a_date][$iduser][] = $event2;                   
        }     
        // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
        //Get records
        
        foreach ($records as $rNum => $r) {
            if ($rNum == 0) {continue;}                     // vynechání hlavičky tabulky
            $r_iduser  = $r[1];
            $r_edited   = $r[6];
            $r_idstatus = $r[3];
            $r_idcall   = $r[5];            
            $r_edited_date = substr($r_edited, 0, 10);
            
            if ($r_edited_date != $date  ||  $r_iduser != $iduser) {continue;} 
                                                            // záznam není ze zkoumaného časového rozsahu nebo se netýká daného uživatele

            // záznam je ze zkoumaného časového rozsahu
            if (!empty($r_idstatus) && !empty($r_idcall))         { $sumary[$date]["recordsTouched"] ++; }
            if (!empty($r_idstatus) && $r_idstatus == '00000021') { $sumary[$date]["recordsDropped"] ++; }      // Zavěsil zákazník
            if (!empty($r_idstatus) && $r_idstatus == '00000122') { $sumary[$date]["recordsTimeout"] ++; }      // Zavěsil systém
            if (!empty($r_idstatus) && $r_idstatus == '00000244') { $sumary[$date]["recordsBusy"]    ++; }      // Obsazeno
            if (!empty($r_idstatus) && $r_idstatus == '00000261') { $sumary[$date]["recordsDenied"]  ++; }      // Odmítnuto
        }            
        $sumary[$date]["activityTime"]      += $users[$date][$iduser]["activityTime"];
        $sumary[$date]["queueSessionTime"]  += $users[$date][$iduser]["queueSession"];
        $sumary[$date]["pauseSessionTime"]  += $users[$date][$iduser]["pauseSession"];
        $sumary[$date]["talkTime"]          += $users[$date][$iduser]["talkTime"];
        $sumary[$date]["callCount"]         += $users[$date][$iduser]["callCount"];
        $sumary[$date]["callCountAnswered"] += $users[$date][$iduser]["callCountAnswered"];
    }                                                    
}                                                           // konec iterace users pro sessions + activities + records + TOTALS 
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// sort pole uživatelů podle počtu hovorů v rámci dnů

foreach ($users as $date => $usersByDay) {
    usort($usersByDay, function($a, $b) {
        return $a["callCount"] < $b["callCount"];
     });
}
// ==============================================================================================================================================================================================
//Do the events magic

foreach ($events as $date => $eventsByDay) {  
    
    foreach ($eventsByDay as $iduser => $evnts) {           // sort pole událostí podle času v rámci dnů
        usort($evnts, function($a, $b) {
            return strcmp($a["time"], $b["time"]);
        });

        $times = [                                          // časy pro uspořádanou dvojici [datum; uživatel]
            "Q"         => 0,
            "QA"        => 0,
            "QAP"       => 0,
            "QP"        => 0,
            "P"         => 0,
            "AP"        => 0
        ];
        $lastTime = 0;
        $status = [
            "Q" => false,
            "A" => false,
            "P" => false
        ];
        foreach ($evnts as $evnt) {
            $currentTime = strtotime($evnt["time"]);
            if ($lastTime > 0) {
                switch ([$status["Q"], $status["A"], $status["P"]]) {                    
                    case [true , true , true ]:     $times["QAP"] += $currentTime - $lastTime;  break;
                    case [true , true , false]:     $times["QA" ] += $currentTime - $lastTime;  break;
                    case [true , false, false]:     $times["Q"  ] += $currentTime - $lastTime;  break;
                    case [true , false, true ]:     $times["QP" ] += $currentTime - $lastTime;  break;
                    case [false, false, true ]:     $times["P"  ] += $currentTime - $lastTime;  break;
                    case [false, true , true ]:     $times["AP" ] += $currentTime - $lastTime;
                    }
                }
            switch ($evnt["method"]) {
                case "+": $status[$evnt["type"]] = true;   break;
                case "-": $status[$evnt["type"]] = false;
            }    
        $lastTime = $currentTime;
        }
        foreach ($times as $evnTyp => $evnTime) {
            $users[$date][$iduser][$evnTyp] = $times[$evnTyp];
        }
    }
}        
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// Count idle time

foreach ($users as $date => $usersByDay) {
    foreach ($usersByDay as $iduser => $usr) {
        $sumary[$date]["idleTime"]         += $usr["Q"];
        $users[$date][$iduser]["idleTime"]  = $usr["Q"];
    }
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                
// Count transactions
/*
foreach ($users as $user) {
    //$userObject = TableRegistry::get('LogUser')->find()->where(['pbx_name' => $user->name, 'idpbxinstance' => 3])->first();
    if (!is_null($userObject)) {
        $user->transactionCount = TableRegistry::get('RewardTransaction')->find()->where(['idloguser' => $userObject->idloguser, 'time >=' => $date . ' 00:00:00', 'time <=' => $date . ' 23:59:59', 'type' => 'O'])->count();
        $sumary->transactionCount += $user->transactionCount;
    }
}
*/
// ==============================================================================================================================================================================================
// zápis záznamů do výstupních souborů       
        
// zápis denních časových úhrnů pro jednotlivé uživatele (produkční výstup)
foreach ($users as $date => $usersByDay) {
    foreach ($usersByDay as $iduser => $user) {
        $colVals = [];
        $colVals[] = $date;
        foreach ($user as $attrVal) {
            $colVals[] = $attrVal;
        }
        $out_users -> writeRow($colVals);
    }
}

// zápis denních sumářů (produkční výstup)
foreach ($sumary as $date => $sumaryByDay) {
    $colVals = [];
    $colVals[] = $date;
    foreach ($sumaryByDay as $datVal) {
        $colVals[] = $datVal;
    }
    $out_sumary -> writeRow($colVals);
}

// zápis událostí (diagnostický výstup)
foreach ($events as $date => $eventsByDay) {
    foreach ($eventsByDay as $iduser => $evnts) {        
        foreach ($evnts as $evntKey => $evnt) {
            $colVals = [];
            $colVals[] = $iduser;
            foreach ($evnt as $evntVal) { 
                $colVals[] = $evntVal;
            }
            $out_events -> writeRow($colVals);
        }        
    }
}

?>