<?php
// PERFORMANCE REPORT ODVOZENÝ Z OUT-BUCKETU DAKTELA

require_once "vendor/autoload.php";

// načtení konfiguračního souboru
$ds         = DIRECTORY_SEPARATOR;
$dataDir    = getenv("KBC_DATADIR");

// pro případ importu parametrů zadaných JSON kódem v definici PHP aplikace v KBC
$configFile = $dataDir."config.json";
$config     = json_decode(file_get_contents($configFile), true);

// ==============================================================================================================================================================================================

$tabsIn = ["queues", "queueSessions", "pauseSessions", "activities", "records"];     // vstupní tabulky

$tabsOut = [                                                        // výstupní tabulky
    "users"     => ["id", "queueSession", "pauseSession", "talkTime", "idleTime", "transactionCount", "activityTime", "callCount", "callCountAnswered"], 
    "userTimes" => ["iduser", "Q", "QA", "QAP", "QP", "P", "AP"],
    "data"      => ["activityTime", "queueSessionTime", "pauseSessionTime", "talkTime", "callCount", "callCountAnswered", "recordsTouched", "recordsDropped",
                    "recordsTimeout", "recordsBusy", "recordsDenied"]
];         
$tabsOutList = array_keys($tabsOut);
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

$date = date('Y-m-d',(strtotime('-1 day', strtotime(date('Y-m-d')))));
$events = [];

//foreach ($queues as $q) {
    //if ($q[0] == $queue) {
        //$this->set('currentQueue', $q);
foreach ($queues as $qNum => $q) {                                  // iterace řádků tabulky front
        if ($qNum == 0) {continue;}                                 // vynechání hlavičky tabulky
        $q_idqueue = (int)$q[0];
                
        //if (!is_null($q_idqueue)) {
            $users = [];                                            // inicializace pole uživatelů
            // Get queue sessions and initiate users
            //$page = 0;
            //while (true) {
            //    $queueSessions = $pbxScript->getData(3, 'queueSessions', [['field' => 'start_time', 'value' => $date . ' 00:00:00', 'operator' => 'gte'], ['field' => 'start_time', 'value' => $date . ' 23:59:59', 'operator' => 'lte']], ['take' => 1000, 'skip' => $page * 1000]);
            foreach ($queueSessions as $qsNum => $qs) {             // foreach ($queueSessions as $qs)
                if ($qsNum == 0) {continue;}                        // vynechání hlavičky tabulky
                $qs_start_time = $qs[1];
                $qs_end_time   = $qs[2];
                $qs_idqueue    = (int)$qs[4];
                $qs_iduser     = (int)$qs[5];
                
                if ($qs_start_time < $date.' 00:00:00' || $qs_start_time > $date.' 23:59:59') {     // záznam není z minulého dne
                    continue;
                }
                if ($qs_idqueue == $q_idqueue) {                    //if ($qs->queue->name == $queue)             
                    if (!in_array($qs[5], array_keys($users))) {    // 5-iduser
                        $user = [                                   // sestavení záznamu do pole uživatelů
                            "id"                => $qs_iduser,
                            "queueSession"      => 0,
                            "pauseSession"      => 0,
                            "talkTime"          => 0,
                            "idleTime"          => 0,
                            "transactionCount"  => 0,
                            "activityTime"      => 0,
                            "callCount"         => 0,
                            "callCountAnswered" => 0
                        ];
                        $users [$qs_iduser] = $user;                // $users[$qs->id_agent->name] = $user
                        $events[$qs_iduser] = [];                   // $events[$qs->id_agent->name] = [];
                    }
                    $user["queueSession"] += (!empty($qs_end_time) ? strtotime($qs_end_time) : time()) - strtotime($qs_start_time);
                    $event1 = [
                        "time"      =>  $qs_start_time,
                        "type"      =>  "Q",
                        "method"    =>  "+"                   
                    ];
                    $event2 = [
                        "time"      =>  !empty($qs_end_time) ? $qs_end_time : date('Y-m-d H:i:s'),
                        "type"      =>  "Q",
                        "method"    =>  "-"                   
                    ];
                    $events[$qs_iduser][] = $event1; 
                    $events[$qs_iduser][] = $event2; 
                }
            }
        /*
                if (count($queueSessions) < 1000) {
                    break;        
                }
            $page++; */

        // Get pause sessions
        foreach ($users as $user) {
            //$pauseSessions = $pbxScript->getData(3, 'pauseSessions', [['field' => 'start_time', 'value' => $date . ' 00:00:00', 'operator' => 'gte'], ['field' => 'start_time', 'value' => $date . ' 23:59:59', 'operator' => 'lte'], ['field' => 'id_agent', 'value' => $user->name, 'operator' => 'eq']], ['take' => 1000]);
            foreach ($pauseSessions as $psNum => $ps) {             // foreach ($pauseSessions as $ps) {
                if ($psNum == 0) {continue;}                        // vynechání hlavičky tabulky
                $ps_start_time = $ps[1];
                $ps_end_time   = $ps[2];
                
                if ($ps_start_time < $date.' 00:00:00' || $ps_start_time > $date.' 23:59:59') {     // záznam není z minulého dne
                    continue;
                }
                $user["pauseSession"] += (!empty($ps_end_time) ? strtotime($ps_end_time) : time()) - strtotime($ps_start_time);
                $event1 = [
                    "time"      =>  $ps_start_time,
                    "type"      =>  "P",
                    "method"    =>  "+"
                ];
                $event2 = [
                    "time"      =>  !empty($ps_end_time) ? $ps_end_time : date('Y-m-d H:i:s'),
                    "type"      =>  "P",
                    "method"    =>  "-"
                ];
                $events[$user["id"]][] = $event1;
                $events[$user["id"]][] = $event2;
            }
        }
        // Get activities
        foreach ($users as $user) {
            //$activities = $pbxScript->getData(3, 'activities', [['field' => 'time', 'value' => $date . ' 00:00:00', 'operator' => 'gte'], ['field' => 'time', 'value' => $date . ' 23:59:59', 'operator' => 'lte'], ['field' => 'user', 'value' => $user->name, 'operator' => 'eq']], ['take' => 1000, 'skip' => $page * 1000]);
            foreach ($activities as $aNum => $a) {                  // foreach ($activities as $a) {
                if ($aNum == 0) {continue;}                         // vynechání hlavičky tabulky
                $a_time       = $a[13];
                $a_type       = $a[10];
                $a_time_open  = $a[15];
                $a_time_close = $a[16];
                $a_item       = $a[19];
                $item         = json_decode($a_item, false);        // dekódováno z JSONu na objekt                
                
                if ($a_time < $date.' 00:00:00' || $a_time > $date.' 23:59:59') {                   // záznam není z minulého dne
                    continue;
                }               
                if ($a_type == 'CALL' && !empty($item)) {
                    $user["activityTime"] += (!empty($a_time_close) ? strtotime($a_time_close) : time()) - strtotime($a_time_open);
                    $user["talkTime"]     += $item-> duration;      // parsuji duration z objektu $item
                    $user["callCount"]    += 1;
                    if ($item-> answered == "true") {               // parsuji answered z objektu $item
                        $user["callCountAnswered"] += 1;
                    }
                }
                $event1 = [
                    "time"      =>  $a_time_open,
                    "type"      =>  "A",
                    "method"    =>  "+"                   
                ];
                $event2 = [
                    "time"      =>  !empty($a_time_close) ? $a_time_close : date('Y-m-d H:i:s'),
                    "type"      =>  "A",
                    "method"    =>  "-"                   
                ];
                $events[$user["id"]][] = $event1;
                $events[$user["id"]][] = $event2;
            }
        }
        //Get records
        $touched = $dropped = $timeout = $denied = $busy = 0;
        //while (true) {
            //$records = $pbxScript->getData(3, 'campaignsRecords', [['field' => 'edited', 'value' => $date . ' 00:00:00', 'operator' => 'gte'], ['field' => 'edited', 'value' => $date . ' 23:59:59', 'operator' => 'lte'], ['field' => 'queue', 'operator' => 'eq', 'value' => $queue]], ['take' => 1000, 'skip' => $page * 1000]);
        foreach ($records as $rNum => $r) {
            if ($rNum == 0) {continue;}                             // vynechání hlavičky tabulky
            $r_edited   = $r[6];
            $r_idstatus = $r[3];
            $r_idcall   = $r[5];
            
            if ($r_edited < $date.' 00:00:00' || $r_edited > $date.' 23:59:59') {                   // záznam není z minulého dne
                    continue;
                }
            if (!empty($r_idstatus) && !empty($r_idcall))         {$touched++;}
            if (!empty($r_idstatus) && $r_idstatus == '00000021') {$dropped++;}     // Zavěsil zákazník
            if (!empty($r_idstatus) && $r_idstatus == '00000122') {$timeout++;}     // Zavěsil systém
            if (!empty($r_idstatus) && $r_idstatus == '00000244') {$busy++;   }     // Obsazeno
            if (!empty($r_idstatus) && $r_idstatus == '00000261') {$denied++; }     // Odmítnuto
        }
            //if (count($records) < 1000) {
            //   break;
            //}
            //$page++;
        //}
        //Count totals
        $data = [];
        $data["activityTime"] = $data["queueSessionTime"] = $data["pauseSessionTime"] = $data["talkTime"] = $data["callCount"] = $data["callCountAnswered"] = $data["idleTime"] = $data["transactionCount"] = 0;
        foreach ($users as $user) {
            $data["activityTime"]     += $user["activityTime"];
            $data["queueSessionTime"] += $user["queueSession"];
            $data["pauseSessionTime"] += $user["pauseSession"];
            $data["talkTime"]         += $user["talkTime"];
            $data["callCount"]        += $user["callCount"];
            $data["callCountAnswered"]+= $user["callCountAnswered"];
            $data["recordsTouched"]   =  $touched;
            $data["recordsDropped"]   =  $dropped;
            $data["recordsTimeout"]   =  $timeout;
            $data["recordsBusy"]      =  $busy;
            $data["recordsDenied"]    =  $denied;
        }
        usort($users, function($a, $b) {
            return $a["callCount"] < $b["callCount"];
        });
        //Do the events magic
        $userTimes = [];
        foreach ($events as $iduser => $evnts) {
            usort($evnts, function($a, $b) {
                return strcmp($a["time"], $b["time"]);
            });
            $times = [
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
            }
            switch ($evnt["method"]) {
                case "+": $status[$evnt["type"]] = true;   break;
                case "-": $status[$evnt["type"]] = false;
            }    
            $lastTime = $currentTime;
            $userTimes[$user["id"]] = $times;
        }        
        //}
        // Count idle time
        foreach ($userTimes as $iduser => $times) {
            $data["idleTime"] += $times["Q"];
            foreach ($users as $user) {
                if ($user["id"] == $iduser) {             // $user->name
                    $user["idleTime"] = $times["Q"];
                }
            }
        }
        /*
        // Count transactions
        foreach ($users as $user) {
            //$userObject = TableRegistry::get('LogUser')->find()->where(['pbx_name' => $user->name, 'idpbxinstance' => 3])->first();
            if (!is_null($userObject)) {
                $user->transactionCount = TableRegistry::get('RewardTransaction')->find()->where(['idloguser' => $userObject->idloguser, 'time >=' => $date . ' 00:00:00', 'time <=' => $date . ' 23:59:59', 'type' => 'O'])->count();
                $data->transactionCount += $user->transactionCount;
            }
        }

        // Set variables
        $this->set('users', $users);
        $this->set('userTimes', $userTimes);
        $this->set('data', $data);
        $this->set('date', $date);    */

        // ==============================================================================================================================================================================================

        // zápis záznamů do výstupních souborů       
        foreach ($users as $usr) {
           $out_users -> writeRow($usr);
        }    
        foreach ($userTimes as $iduser => $times) {
            $colVals = [
               $iduser,
               $times["Q"],
               $times["QA"],
               $times["QAP"],
               $times["QP"],
               $times["P"],
               $times["AP"]
            ];
            $out_userTimes -> writeRow($colVals);
        }    

        $out_data -> writeRow($data);   
    //}
}
?>