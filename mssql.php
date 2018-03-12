<?php

/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */

/**
 * The PEAR DB driver for PHP's mssql extension
 * for interacting with Microsoft SQL Server databases
 *
 * PHP versions 4 and 5
 *
 * LICENSE: This source file is subject to version 3.0 of the PHP license
 * that is available through the world-wide-web at the following URI:
 * http://www.php.net/license/3_0.txt.  If you did not receive a copy of
 * the PHP License and are unable to obtain it through the web, please
 * send a note to license@php.net so we can mail you a copy immediately.
 *
 * @category   Database
 * @package    DB
 * @author     Sterling Hughes <sterling@php.net>
 * @author     Daniel Convissor <danielc@php.net>
 * @copyright  1997-2005 The PHP Group
 * @license    http://www.php.net/license/3_0.txt  PHP License 3.0
 * @version    CVS: $Id: mssql.php,v 1.90 2007/01/12 05:16:22 aharvey Exp $
 * @link       http://pear.php.net/package/DB
 */

/**
 * Obtain the DB_common class so it can be extended from
 */
require_once 'DB/common.php';

/**
 * The methods PEAR DB uses to interact with PHP's mssql extension
 * for interacting with Microsoft SQL Server databases
 *
 * These methods overload the ones declared in DB_common.
 *
 * DB's mssql driver is only for Microsfoft SQL Server databases.
 *
 * If you're connecting to a Sybase database, you MUST specify "sybase"
 * as the "phptype" in the DSN.
 *
 * This class only works correctly if you have compiled PHP using
 * --with-mssql=[dir_to_FreeTDS].
 *
 * @category   Database
 * @package    DB
 * @author     Sterling Hughes <sterling@php.net>
 * @author     Daniel Convissor <danielc@php.net>
 * @copyright  1997-2005 The PHP Group
 * @license    http://www.php.net/license/3_0.txt  PHP License 3.0
 * @version    Release: 1.7.9
 * @link       http://pear.php.net/package/DB
 */
class DB_mssql extends DB_common
{
    // {{{ properties

    /**
     * The DB driver type (mysql, oci8, odbc, etc.)
     * @var string
     */
    var $phptype = 'mssql';

    /**
     * The database syntax variant to be used (db2, access, etc.), if any
     * @var string
     */
    var $dbsyntax = 'mssql';

    /**
     * The capabilities of this DB implementation
     *
     * The 'new_link' element contains the PHP version that first provided
     * new_link support for this DBMS.  Contains false if it's unsupported.
     *
     * Meaning of the 'limit' element:
     *   + 'emulate' = emulate with fetch row by number
     *   + 'alter'   = alter the query
     *   + false     = skip rows
     *
     * @var array
     */
    var $features = array(
        'limit'         => 'emulate',
        'new_link'      => false,
        'numrows'       => true,
        'pconnect'      => true,
        'prepare'       => false,
        'ssl'           => false,
        'transactions'  => true,
    );

    /**
     * A mapping of native error codes to DB error codes
     * @var array
     */
    // XXX Add here error codes ie: 'S100E' => DB_ERROR_SYNTAX
    var $errorcode_map = array(
        102   => DB_ERROR_SYNTAX,
        110   => DB_ERROR_VALUE_COUNT_ON_ROW,
        155   => DB_ERROR_NOSUCHFIELD,
        156   => DB_ERROR_SYNTAX,
        170   => DB_ERROR_SYNTAX,
        207   => DB_ERROR_NOSUCHFIELD,
        208   => DB_ERROR_NOSUCHTABLE,
        245   => DB_ERROR_INVALID_NUMBER,
        319   => DB_ERROR_SYNTAX,
        321   => DB_ERROR_NOSUCHFIELD,
        325   => DB_ERROR_SYNTAX,
        336   => DB_ERROR_SYNTAX,
        515   => DB_ERROR_CONSTRAINT_NOT_NULL,
        547   => DB_ERROR_CONSTRAINT,
        1018  => DB_ERROR_SYNTAX,
        1035  => DB_ERROR_SYNTAX,
        1913  => DB_ERROR_ALREADY_EXISTS,
        2209  => DB_ERROR_SYNTAX,
        2223  => DB_ERROR_SYNTAX,
        2248  => DB_ERROR_SYNTAX,
        2256  => DB_ERROR_SYNTAX,
        2257  => DB_ERROR_SYNTAX,
        2627  => DB_ERROR_CONSTRAINT,
        2714  => DB_ERROR_ALREADY_EXISTS,
        3607  => DB_ERROR_DIVZERO,
        3701  => DB_ERROR_NOSUCHTABLE,
        7630  => DB_ERROR_SYNTAX,
        8134  => DB_ERROR_DIVZERO,
        9303  => DB_ERROR_SYNTAX,
        9317  => DB_ERROR_SYNTAX,
        9318  => DB_ERROR_SYNTAX,
        9331  => DB_ERROR_SYNTAX,
        9332  => DB_ERROR_SYNTAX,
        15253 => DB_ERROR_SYNTAX,
    );

    /**
     * The raw database connection created by PHP
     * @var resource
     */
    var $connection;

    /**
     * The DSN information for connecting to a database
     * @var array
     */
    var $dsn = array();


    /**
     * Should data manipulation queries be committed automatically?
     * @var bool
     * @access private
     */
    var $autocommit = true;

    /**
     * The quantity of transactions begun
     *
     * {@internal  While this is private, it can't actually be designated
     * private in PHP 5 because it is directly accessed in the test suite.}}
     *
     * @var integer
     * @access private
     */
    var $transaction_opcount = 0;

    /**
     * The database specified in the DSN
     *
     * It's a fix to allow calls to different databases in the same script.
     *
     * @var string
     * @access private
     */
    var $_db = null;


    // }}}
    // {{{ constructor

    /**
     * This constructor calls <kbd>$this->DB_common()</kbd>
     *
     * @return void
     */
    function DB_mssql()
    {
        $this->DB_common();
    }

    // }}}
    // {{{ connect()

    /**
     * Connect to the database server, log in and open the database
     *
     * Don't call this method directly.  Use DB::connect() instead.
     *
     * @param array $dsn         the data source name
     * @param bool  $persistent  should the connection be persistent?
     *
     * @return int  DB_OK on success. A DB_Error object on failure.
     */
    function connect($dsn, $persistent = false)
    {
        if (!PEAR::loadExtension('mssql') && !PEAR::loadExtension('sybase')
            && !PEAR::loadExtension('sybase_ct'))
        {
            return $this->raiseError(DB_ERROR_EXTENSION_NOT_FOUND);
        }

        $this->dsn = $dsn;
        if ($dsn['dbsyntax']) {
            $this->dbsyntax = $dsn['dbsyntax'];
        }

        $params = array(
            $dsn['hostspec'] ? $dsn['hostspec'] : 'localhost',
            $dsn['username'] ? $dsn['username'] : null,
            $dsn['password'] ? $dsn['password'] : null,
        );
        if ($dsn['port']) {
            $params[0] .= ((substr(PHP_OS, 0, 3) == 'WIN') ? ',' : ':')
                        . $dsn['port'];
        }

        $connect_function = $persistent ? 'mssql_pconnect' : 'mssql_connect';

        $this->connection = @call_user_func_array($connect_function, $params);

        if (!$this->connection) {
            return $this->raiseError(DB_ERROR_CONNECT_FAILED,
                                     null, null, null,
                                     @mssql_get_last_message());
        }
        if ($dsn['database']) {
            if (!@mssql_select_db($dsn['database'], $this->connection)) {
                return $this->raiseError(DB_ERROR_NODBSELECTED,
                                         null, null, null,
                                         @mssql_get_last_message());
            }
            $this->_db = $dsn['database'];
        }
        return DB_OK;
    }

    // }}}
    // {{{ disconnect()

    /**
     * Disconnects from the database server
     *
     * @return bool  TRUE on success, FALSE on failure
     */
    function disconnect()
    {
        $ret = @mssql_close($this->connection);
        $this->connection = null;
        return $ret;
    }

    // }}}
    // {{{ simpleQuery()

    /**
     * Sends a query to the database server
     *
     * @param string  the SQL query string
     *
     * @return mixed  + a PHP result resrouce for successful SELECT queries
     *                + the DB_OK constant for other successful queries
     *                + a DB_Error object on failure
     */
    function simpleQuery($query)
    {
        $ismanip = $this->_checkManip($query);
        $this->last_query = $query;
        if (!@mssql_select_db($this->_db, $this->connection)) {
            return $this->mssqlRaiseError(DB_ERROR_NODBSELECTED);
        }
		
        $query = $this->modifyQuery($query);
		
        if (!$this->autocommit && $ismanip) {
            if ($this->transaction_opcount == 0) {
                $result = @mssql_query('BEGIN TRAN', $this->connection);
                if (!$result) {
                    return $this->mssqlRaiseError();
                }
            }
            $this->transaction_opcount++;
        }
        $result = @mssql_query($query, $this->connection);
        if (!$result) {
            return $this->mssqlRaiseError();
        }
        // Determine which queries that should return data, and which
        // should return an error code only.
        return $ismanip ? DB_OK : $result;
    }
	
	function modifyQuery($query)
    {
						
		$query = str_replace("`", "", $query);
		$query = str_replace( array('[',']') , ''  , $query );

		$tablesName = $this->getTableName($query);	
		
		$tableNamesWithoutAlise = $this->removeAliseName($tablesName);
		
		$tableNamesWithPrefix = $this->prefixDBName($tableNamesWithoutAlise);

		// Group By Function Call
		$query = $this->getGroupBy($query);
		
		// Order By Function Call
		$query = $this->getOrderBy($query);
		
		//replace Limit keyword with TOP keyword
		$query = $this->getLimit($query);
		
		// MSSQLDateFormat function call
		$query = $this->getMSSQLDateFormat($query);
		
		/*force index => WITH index*/
		preg_match_all("#\sforce[\s]*index[\s]*\(\w*\)[\s]*#i",$query,$forceIndex);
		if(!empty($forceIndex['0']['0']))
		{
			$forceString = $forceIndex['0']['0'].')';
			$forceString = str_ireplace('force index','WITH (index',$forceString);
		}
		// Changed use index to with index	
		preg_match_all("#\suse[\s]*index[\s]*\(\w*\)[\s]*#i",$query,$useIndex);
		if(!empty($useIndex['0']['0']))
		{
			$useString = $useIndex['0']['0'].')';
			$useString = str_ireplace('use index','WITH (index',$useString);
		}
		/*Interval 10 date*/
		preg_match_all("#INTERVAL\s[0-9]*\sday#i",$query,$intervalIndex);
		if(!empty($intervalIndex[0][0]))
		{
			$intervalString = explode(" ",$intervalIndex[0][0]);
			$intervalString = $intervalString[1];
		} else {
			$intervalString = '';
		}
			
		// Patterns
		$patterns[0] ='#(?<![\w])secure_pgatxn.(?![\w][.])#i';
		$patterns[1] ='#(?<![\w])secure_pga.(?![\w][.])#i';
		$patterns[2] ='#\sIFNULL#i';
		$patterns[3] = '#[\s]{1,}WHERE[\s]{1,}1#i';// where 1 should be replaced with WHERE 1=1		
		$patterns[4] ='#\sforce index[\s]*\(\w*\)[\s]*#i';// Force Index is replaced with With Index
		$patterns[5] ='#\suse\sindex[\s]*\(\w*\)[\s]*#i';// use Index is replaced with with index
		$patterns[6] ='#[\s]*&&{1}[\s]*#i';
		$patterns[7] = '#IF[\s]*\([\s]*#i';// IF should be replaced with where IIF
		$patterns[8] = '#now\([\s]*#i';// Now() function should be replaced with getutcdate()
		$patterns[10] = '#[\W]DATE[\s]*\([\s]*#i';// Mysql Date() is changed to  CONVERT (date,GETDATE())		
		$patterns[11] = '#DATE_FORMAT[\s]*\([\s]*#i';// date_format() function should be replaced with format
		$patterns[12] = '#[\s]*(%Y{1}-%m{1}-%d{1})#i';// Change '%Y-%m-%d' patteren to 'yyyy-MM-dd'
        $patterns[13] = '#%m{1} %d{1}#i';// Change '%m %d' patteren to 'MM dd'  \'
		$patterns[14] = '#DATE_SUB\([\s]*#i';// DATE_SUB() function accepts 3 parameters// curdate
		$patterns[15] = '#CURDATE\([\s]*#i';// DATE_SUB() function accepts 3 parameters// curdate
		$patterns[16] ='#\,[\s]*INTERVAL\s[0-9]*\sDAY#i';// INTERVAL 1 DAY is replaced with ''
		$patterns[17] ='#[\s]*DATE_ADD\([\s]*#i';// Mysql Date() is changed to  CONVERT (date,GETDATE())
		$patterns[18] ='#[\s]*\|\|[\s]*#i'; // || to OR 
		$patterns[19] ='#(?<![\w])secure_lib.(?![\w][.])#i';
		$patterns[20] = '#UCASE\([\s]*#i';
		
		// Replacement Queries
		$replacements[0] = ' '; 
        $replacements[1] = ' '; 
		$replacements[2] = ' ISNULL';
		$replacements[3] = ' WHERE 1=1 ';
		$replacements[4] = isset($forceString)?$forceString:''.' '; 
		$replacements[5] = isset($useString)?$useString:''.' '; 
		$replacements[6] = ' AND ';
		$replacements[7] = 'IIF('; 
		$replacements[8] = 'GETDATE(';
		$replacements[10] = ' CONVERT(date,';		
		$replacements[11] = 'FORMAT(';
		$replacements[12] = 'yyyy-MM-dd'; 
        $replacements[13] = 'MM dd';
		$replacements[14] = ' DATEADD(DAY,-'.$intervalString.','; 
		$replacements[15] = 'GETDATE('; 
		$replacements[16] = '';
		$replacements[17] = ' DATEADD(DAY,'.$intervalString.',';
		$replacements[18] = ' OR ';
		$replacements[19] = ' '; 
		$replacements[20] = 'UPPER(';
		
		$query = preg_replace($patterns, $replacements, $query);
		$query = $this->strReplaceTableName($query, $tableNamesWithPrefix); 
		return $query;
		
    }
	
	function getLimit($query)
	{
		preg_match_all("#LIMIT[^\w]{1,}([0-9]{1,})[\s]*([\,]{0,})[\s]*([0-9]{0,})#i",$query,$matches);
		$patterns = '#LIMIT[^\w]{1,}([0-9]{1,})[\s]*([\,]{0,})[\s]*([0-9]{0,})#i';
		$replacements = '';
		$query = preg_replace($patterns, $replacements, $query);
		if(!empty($matches[1][0]) && empty($matches[3][0]))
		{
			$query = str_ireplace("SELECT ", "SELECT TOP ".$matches[1][0]." ", $query);
		}
		else if(!empty($matches[3][0]))
		{
			$limitQuery = " OFFSET  ".$matches[1][0]." ROWS FETCH NEXT ".$matches[3][0]." ROWS ONLY";
			if(stripos($query, "ORDER BY"))
				{
					$query .= $limitQuery;
				}
				else
				{
 				$selectList = $this->selectList($query,"SELECT","FROM");
				$selectList = $this->sqlParser($selectList);
				$selectList = preg_replace('#[\s]as[\s]\w*#i', " ", $selectList); 
				$query .= " ORDER BY ".$selectList[0].$limitQuery;
				}
		}

		return $query;
	}
	
	function getMSSQLDateFormat($query)
	{
			if(stripos($query, "%b %d"))
			{
				preg_match_all("#DATE_FORMAT[\s]*\([\s]*[\w].*[,]{1}[\s]*\'%b{1}[\s]*%d{1}\'[\s]*\)#iU",$query,$dateFormat);
				if(!empty($dateFormat))
					{
						$columnName = explode(",",$dateFormat[0][0]);
						$columnName = str_ireplace("date_format(","",$columnName[0]);
						$query = str_ireplace($dateFormat[0][0],"convert(char(3), ".$columnName.", 0)+' '+CONVERT(char(2), ".$columnName.", 4)",$query);
					}
				return $query;
			}
			else if(strpos($query, "%Y-%M"))
			{
				preg_match_all("#DATE_FORMAT[\s]*\([\s]*[\w].*[,]{1}[\s]*\'%Y{1}[\s]*-%M{1}\'[\s]*\)#iU",$query,$dateFormat);
				$columnName = explode(",",$dateFormat[0][0]);
				$columnName = str_ireplace("date_format(","",$columnName[0]);
				$query = str_ireplace($dateFormat[0][0],"DATENAME(year,".$columnName.")+'-'+DATENAME(month,".$columnName.")",$query);
				return $query;
			}
			 else if(stripos($query, "%Y-%m"))
			{
				preg_match_all("#DATE_FORMAT[\s]*\([\s]*[\w].*[,]{1}[\s]*\'%Y{1}[\s]*-%m{1}\'[\s]*\)#iU",$query,$dateFormat);
				$columnName = explode(",",$dateFormat[0][0]);
				$columnName = str_ireplace("date_format(","",$columnName[0]);
				$query = str_ireplace($dateFormat[0][0],"CONVERT(VARCHAR(7), ".$columnName.", 120)",$query);
				return $query;
			}
			  else if(stripos($query, "%M %d, %Y") || stripos($query, "%M %d. %Y"))
			{
				preg_match_all("#DATE_FORMAT[\s]*\([\s]*[\w].*[,]{1}[\s]*\'%M{1} %d{1}[\,|\.][\s]*%Y{1}\'[\s]*\)#iU",$query,$dateFormat);
				foreach($dateFormat[0] as $key => $value)
				{
					$columnName = explode(",",$dateFormat[0][$key]);
					$columnName = str_ireplace("date_format(","",$columnName[0]);
					$query = str_ireplace($dateFormat[0][$key],"CONVERT(VARCHAR(12), ".$columnName.", 107)",$query);
					
					
				}
				return $query;
				
				
			}
			 else if(stripos($query, "%d/%m/%Y %H:%i:%S"))
			{
				
				preg_match_all("#DATE_FORMAT[\s]*\([\s]*[\w].*[,]{1}[\s]*\'\%d/\%m/\%Y[\s]*\%H:\%i:\%S\'[\s]*\)#iU",$query,$dateFormat);
				$columnName = explode(",",$dateFormat[0][0]);
				$columnName = str_ireplace("date_format(","",$columnName[0]);
				$query = str_ireplace($dateFormat[0][0],"CONVERT(VARCHAR(10), ".$columnName.", 103)+' '+CONVERT(VARCHAR(18),".$columnName.",108)",$query);
				return $query;
				
				
			}
			 else if(stripos($query, "%d%m%Y"))
			{
				preg_match_all("#DATE_FORMAT[\s]*\([\s]*[\w]*.*\)#i",$query,$dateFormat);
				$columnName = explode(",",$dateFormat[0][0]);
				$columnName = str_ireplace("date_format(","",$columnName[0]);
				$query = str_ireplace($dateFormat[0][0]," REPLACE(CONVERT(VARCHAR(10),".$columnName.",103), '/','')",$query);
				return $query;
				
				
			}
			
			 else if(stripos($query, "%d/%m/%Y"))
			{
				preg_match_all("#DATE_FORMAT[\s]*\([\s]*[\w].*[,]{1}[\s]*\'\%d{1}/\%m{1}/\%Y{1}\'[\s]*\)#iU",$query,$dateFormat);
				$columnName = explode(",",$dateFormat[0][0]);
				$columnName = str_ireplace("date_format(","",$columnName[0]);
				$query = str_ireplace($dateFormat[0][0],"CONVERT(VARCHAR(10), ".$columnName.", 103)",$query);
				return $query;
				
				
			}
			 else if(stripos($query, "%d %M,%Y"))
			{
				
				preg_match_all("#DATE_FORMAT[\s]*\([\s]*[\w].*[,]{1}[\s]*\'\%d{1}[\s]*\%M{1}[,]\%Y{1}\'[\s]*\)#iU",$query,$dateFormat);
				$columnName = explode(",",$dateFormat[0][0]);
				$columnName = str_ireplace("date_format(","",$columnName[0]);
				$query = str_ireplace($dateFormat[0][0],"CONVERT(VARCHAR(11), ".$columnName.", 106)",$query);
				return $query;
				
				
			}
			 else if(stripos($query, "%H:00"))
			{
				
				preg_match_all("#DATE_FORMAT[\s]*\([\s]*[\w].*[,]{1}[\s]*\'[\s]*\%H{1}:00[\s]*\'[\s]*\)#iU",$query,$dateFormat);
				$columnName = explode(",",$dateFormat[0][0]);
				$columnName = str_ireplace("date_format(","",$columnName[0]);
				$query = str_ireplace($dateFormat[0][0],"RIGHT(100 + DATEPART(HOUR, ".$columnName."),2)+':00'",$query);


				return $query;
				
				
			}
			 else if(stripos($query, "datediff"))
			{
				preg_match_all("#datediff[\s]*\([\s]*[\w].*[,]{1}[\s]*[\w].*[\s]*\)#iU",$query,$dateDiff);
				
				if (isset($dateDiff[0])) {
					
					foreach($dateDiff[0] as $key => $value)
					{
						$columnName = explode(",",$dateDiff[0][$key]);
						if (count($columnName) < 3) {
							$Param1 = substr($columnName[1],0,-1);
							$Param2 = str_ireplace("datediff("," ",$columnName[0]);	
							$query = str_ireplace($dateDiff[0][$key],"datediff(DAY,$Param1,$Param2)",$query);
						}
					}
				}
				
				return $query;
			}
			
			else
			{
				return $query;
			}
	}
	
	function getOrderBy($query)
	{
		/*Order By functionality starts*/
		preg_match_all("#order[\s]by[^\w]{1,}([0-9]{1,})([\,]{0,})([0-9]{0,})#i",$query,$orderByNum);
		
		if(!empty($orderByNum[0]))
		{
		$selectList = $this->selectList($query,"SELECT","FROM");
		$orderByList = $this->orderByList($query,"ORDER BY","DESC");
		
		$selectList = $this->sqlParser($selectList);
		
		$selectList = preg_replace('#[\s]as[\s]{1,}\w*#i', " ", $selectList);
		$orderByArr = explode(",",$orderByList);
		$orderByArr = array_map('trim',$orderByArr);
		/**Code for gropup by 1*/
		if(!empty($orderByNum[0])) 
		{
			$orderByCol=array();
			foreach($orderByArr as $colno)
			{
						$orderByCol[]=$selectList[$colno-1];
			}
		}
		if(!empty($orderByNum[0]))
		{
			$orderBy = implode(",",$orderByCol);
		}

		if(stripos($query, "ORDER BY"))
		{
			$query = preg_replace('#order[\s]by[^\w]{1,}([0-9]{1,})([\,]{0,})([0-9]{0,})([\,]{0,})([0-9]{0,})([\,]{0,})([0-9]{0,})([\,]{0,})([0-9]{0,})([\,]{0,})([0-9]{0,})([\,]{0,})([0-9]{0,})([\,]{0,})([0-9]{0,})([\,]{0,})([0-9]{0,})#i', "ORDER BY $orderBy ", $query);					
		}
		}
		return $query;
		/*Ends here*/
	}
	
	function getGroupBy($query)
	{
		preg_match_all("#group[\s]by\s#i",$query,$groupByWord);
		preg_match_all("#group[\s]by[^\w]{1,}([0-9]{1,})([\,]{0,})([0-9]{0,})#i",$query,$groupByNum);
		$groupBy = !empty($groupByWord[0])?$groupByWord[0]:$groupByNum[0];
		if(!empty($groupBy))
		{
		$selectList = $this->selectList($query,"SELECT","FROM");
		$groupByList = $this->groupByList($query);
		$selectList = $this->sqlParser($selectList);
		$selectList = preg_replace("#[\s]as[\s]{1,}\w*#i", " ", $selectList);
		$selectList = $this->removeAggregateFunc($selectList);
		$groupByArr = explode(",",$groupByList);
		$groupByArr = array_map('trim',$groupByArr);
		
		/**Code for gropup by 1*/
		if(!empty($groupByNum[0]))
		{
			$groupByCol=array();
			foreach($groupByArr as $colno)
			{
				
					$groupByCol[]=$selectList[$colno-1];
			}
		}
		if(!empty($groupByNum[0]))
		{
			$combineVal = array_merge_recursive($groupByCol,$selectList);	
			$combineVal = array_map('trim',$combineVal);
			$combineVal = array_unique($combineVal);
			$groupBy = implode(",",$combineVal);
		}
		else
		{
			$combineVal = array_merge_recursive($groupByArr,$selectList);	
			$combineVal = array_map('trim',$combineVal);
			$combineVal = array_unique($combineVal);
			$groupBy = implode(",",$combineVal);
		}
		$trimmed_array=array_map('trim',$groupByArr);
		if(stripos($query, "ORDER BY"))
		{
			$query = preg_replace('#GROUP BY[\s\S]+?ORDER BY#i', "GROUP BY $groupBy ORDER BY ", $query);
		}
		else
		{
			$query = preg_replace('#GROUP BY[\s]{1,}.*#i', "GROUP BY ".$groupBy, $query);
		}
		
		
		}
		return $query;
	}
	
	function removeAggregateFunc($selectList)
	{
		if(!empty($selectList))
		{
			foreach($selectList as $key => $val)
			{
				if(stripos($val, "sum(") !== false)
				{
					unset($selectList[$key]);
					
				}
				else if(stripos($val, "count(") !== false)
				{
					unset($selectList[$key]);
				}
				else if(stripos($val, "MAX(") !== false)
				{
					unset($selectList[$key]);
				}
				else if(stripos($val, "MIN(") !== false)
				{
					unset($selectList[$key]);
				}
				else if(stripos($val, "AVG(") !== false)
				{
					unset($selectList[$key]);
				}
			}
			
		}
		return $selectList;
		
	}
	
	function groupByList($string){
		
        if(stripos($string, "ORDER BY"))
		{
			preg_match_all("#group\sby(?s)(.*)order\sby#i",$string,$groupByOrderby);
			return $groupByOrderby[1][0];
		}
		else if(stripos($string, "limit"))
		{
			preg_match_all("#group\sby(?s)(.*)\slimit#i",$string,$groupByLimit);
			return 	$groupByLimit[1][0];
		}
		else if(stripos($string, "having"))
		{
			preg_match_all("#group\sby(?s)(.*)\shaving#iU",$string,$groupByHaving);
			return 	$groupByHaving[1][0];
		}
		else
		{
			preg_match_all("#group\sby(?s)(.*)#i",$string,$groupBy);
			return $groupBy[1][0];
		}
    }
	function orderByList($string, $start, $end){
		
        $string = ' ' . $string;
        $ini = stripos($string, $start);
        if ($ini == 0) return '';
        $ini += strlen($start);
		
		if(stripos($string, "DESC") === false)
		{
			return substr($string, $ini);
		}
		else
		{
			$len = stripos($string, $end, $ini) - $ini;
			return substr($string, $ini, $len);
		}
    }
	
	function selectList($string, $start, $end){
		preg_match_all("#select(?s)(.*)[\W]from#iU",$string,$selectFrom);
		return $selectFrom[1][0];
    }
	
	function sqlParser($str)
	{
		$str = str_split($str);
			$tokens = array();
			$token = "";
			$stack = array();
			foreach($str as $char) {
				if($char == "," && empty($stack)) {
					$tokens[] = trim($token);
					$token = "";
				} else {
					$token = $token .$char;
					if($char == '(') {
						array_unshift($stack, $char);
					}
					
					if($char == ')') {
						array_shift($stack);
					}
				}
				
			}

			$tokens[] = trim($token);
			return $tokens;
	}
	
	function getTableName($query)
	{
		$tables = array();
		$query_structure = explode( ' ', strtolower( preg_replace('!\s+!', ' ', $query) ) );
		$searches_from = array_keys( $query_structure , 'from');
		$searches_join = array_keys( $query_structure , 'join');
		$searches_update = array_keys( $query_structure , 'update');
		$searches_into = array_keys( $query_structure , 'into');

		$searches = array_merge($searches_join , $searches_from , $searches_update , $searches_into);
		foreach($searches as $search ){
			if(isset($query_structure[$search+1])){
				$tables[] = trim( $query_structure[$search+1] , '` ');
			}
		}
	
		$patterns[1] ='#(?<![\w])secure_pgatxn.(?![\w][.])#i';
		$patterns[2] ='#(?<![\w])secure_pga.(?![\w][.])#i';
		$patterns[3] ='#(?<![\w])secure_lib.(?![\w][.])#i';
	
		$replacements[1] = '';   
		$replacements[2] = '';
		$replacements[3] = '';
	
		$tables = preg_replace($patterns, $replacements,$tables);
		
		return $tables;
	}
	

	
	
	function prefixDBName($tableList)
	{
	  
	  	$tableArray = array(
		"job_queue_notification_log"=>"secure_pga.dbo.job_queue_notification_log"
		,"old_pga_customer_card"=>"secure_pga.dbo.old_pga_customer_card"
		,"pga_account"=>"secure_pga.dbo.pga_account"
		,"pga_account_activation_history"=>"secure_pga.dbo.pga_account_activation_history"
		,"pga_account_amc_settings"=>"secure_pga.dbo.pga_account_amc_settings"
		,"pga_account_amc_settings_history"=>"secure_pga.dbo.pga_account_amc_settings_history"
		,"pga_account_bms_config"=>"secure_pga.dbo.pga_account_bms_config"
		,"pga_account_customer"=>"secure_pga.dbo.pga_account_customer"
		,"pga_account_customer_card"=>"secure_pga.dbo.pga_account_customer_card"
		,"pga_account_discount"=>"secure_pga.dbo.pga_account_discount"
		,"pga_account_emi_history"=>"secure_pga.dbo.pga_account_emi_history"
		,"pga_account_history"=>"secure_pga.dbo.pga_account_history"
		,"pga_account_listeners"=>"secure_pga.dbo.pga_account_listeners"
		,"pga_account_live"=>"secure_pga.dbo.pga_account_live"
		,"pga_account_notification"=>"secure_pga.dbo.pga_account_notification"
		,"pga_account_offline_emi_history"=>"secure_pga.dbo.pga_account_offline_emi_history"
		,"pga_account_post_api_setting"=>"secure_pga.dbo.pga_account_post_api_setting"
		,"pga_account_refund_history"=>"secure_pga.dbo.pga_account_refund_history"
		,"pga_account_request"=>"secure_pga.dbo.pga_account_request"
		,"pga_account_rm"=>"secure_pga.dbo.pga_account_rm"
		,"pga_account_rm_mapping"=>"secure_pga.dbo.pga_account_rm_mapping"
		,"pga_account_sch_report_setting"=>"secure_pga.dbo.pga_account_sch_report_setting"
		,"pga_account_setting"=>"secure_pga.dbo.pga_account_setting"
		,"pga_account_setting_live"=>"secure_pga.dbo.pga_account_setting_live"
		,"pga_account_shipping"=>"secure_pga.dbo.pga_account_shipping"
		,"pga_account_suspend_history"=>"secure_pga.dbo.pga_account_suspend_history"
		,"pga_account_tdr"=>"secure_pga.dbo.pga_account_tdr"
		,"pga_account_tdr2"=>"secure_pga.dbo.pga_account_tdr2"
		,"pga_account_tdr_31-dec-2017"=>"secure_pga.dbo.pga_account_tdr_31-dec-2017"
		,"pga_account_tdr_final"=>"secure_pga.dbo.pga_account_tdr_final"
		,"pga_account_tdr_live"=>"secure_pga.dbo.pga_account_tdr_live"
		,"pga_account_tdr_original"=>"secure_pga.dbo.pga_account_tdr_original"
		,"pga_account_tdr_temp"=>"secure_pga.dbo.pga_account_tdr_temp"
		,"pga_account_vpc_history"=>"secure_pga.dbo.pga_account_vpc_history"
		,"pga_acctdr_history"=>"secure_pga.dbo.pga_acctdr_history"
		,"pga_activity"=>"secure_pga.dbo.pga_activity"
		,"pga_activity_live"=>"secure_pga.dbo.pga_activity_live"
		,"pga_amex_acceptance"=>"secure_pga.dbo.pga_amex_acceptance"
		,"pga_auto_pay_batch"=>"secure_pga.dbo.pga_auto_pay_batch"
		,"pga_background_reports"=>"secure_pga.dbo.pga_background_reports"
		,"pga_bank"=>"secure_pga.dbo.pga_bank"
		,"pga_bank_accounts"=>"secure_pga.dbo.pga_bank_accounts"
		,"pga_bank_bins"=>"secure_pga.dbo.pga_bank_bins"
		,"pga_basic_route"=>"secure_pga.dbo.pga_basic_route"
		,"pga_brand"=>"secure_pga.dbo.pga_brand"
		,"pga_buyrate_config"=>"secure_pga.dbo.pga_buyrate_config"
		,"pga_buyrate_config_details"=>"secure_pga.dbo.pga_buyrate_config_details"
		,"pga_buyrate_config_history"=>"secure_pga.dbo.pga_buyrate_config_history"
		,"pga_card_bin"=>"secure_pga.dbo.pga_card_bin"
		,"pga_card_brand"=>"secure_pga.dbo.pga_card_brand"
		,"pga_card_country_bin"=>"secure_pga.dbo.pga_card_country_bin"
		,"pga_card_issuer_details"=>"secure_pga.dbo.pga_card_issuer_details"
		,"pga_case_rules"=>"secure_pga.dbo.pga_case_rules"
		,"pga_category"=>"secure_pga.dbo.pga_category"
		,"pga_category_mecode"=>"secure_pga.dbo.pga_category_mecode"
		,"pga_ccfd"=>"secure_pga.dbo.pga_ccfd"
		,"pga_citi_arn_txn"=>"secure_pga.dbo.pga_citi_arn_txn"
		,"pga_citi_arn_upload"=>"secure_pga.dbo.pga_citi_arn_upload"
		,"pga_citi_codes"=>"secure_pga.dbo.pga_citi_codes"
		,"pga_currency_log"=>"secure_pga.dbo.pga_currency_log"
		,"pga_customer"=>"secure_pga.dbo.pga_customer"
		,"pga_customer_card"=>"secure_pga.dbo.pga_customer_card"
		,"pga_customer_device"=>"secure_pga.dbo.pga_customer_device"
		,"pga_customer_transaction"=>"secure_pga.dbo.pga_customer_transaction"
		,"pga_cust_payment"=>"secure_pga.dbo.pga_cust_payment"
		,"pga_debitcard_bins"=>"secure_pga.dbo.pga_debitcard_bins"
		,"pga_domains"=>"secure_pga.dbo.pga_domains"
		,"pga_error_codes_DRC"=>"secure_pga.dbo.pga_error_codes_DRC"
		,"pga_gateway"=>"secure_pga.dbo.pga_gateway"
		,"pga_gatewayparam_setting"=>"secure_pga.dbo.pga_gatewayparam_setting"
		,"pga_gateway_12-dec-2017_1"=>"secure_pga.dbo.pga_gateway_12-dec-2017_1"
		,"pga_gateway_account_config"=>"secure_pga.dbo.pga_gateway_account_config"
		,"pga_gateway_account_config_value"=>"secure_pga.dbo.pga_gateway_account_config_value"
		,"pga_gateway_account_upload"=>"secure_pga.dbo.pga_gateway_account_upload"
		,"pga_gateway_account_upload_record"=>"secure_pga.dbo.pga_gateway_account_upload_record"
		,"pga_gateway_config"=>"secure_pga.dbo.pga_gateway_config"
		,"pga_gateway_documents"=>"secure_pga.dbo.pga_gateway_documents"
		,"pga_gateway_history"=>"secure_pga.dbo.pga_gateway_history"
		,"pga_gateway_limit"=>"secure_pga.dbo.pga_gateway_limit"
		,"pga_gateway_live"=>"secure_pga.dbo.pga_gateway_live"
		,"pga_gateway_response"=>"secure_pga.dbo.pga_gateway_response"
		,"pga_gateway_tdr"=>"secure_pga.dbo.pga_gateway_tdr"
		,"pga_gateway_to_account"=>"secure_pga.dbo.pga_gateway_to_account"
		,"pga_gateway_to_account_31-dec-2017"=>"secure_pga.dbo.pga_gateway_to_account_31-dec-2017"
		,"pga_gateway_to_account_live"=>"secure_pga.dbo.pga_gateway_to_account_live"
		,"pga_gateway_to_account_new"=>"secure_pga.dbo.pga_gateway_to_account_new"
		,"pga_gateway_to_account_temp"=>"secure_pga.dbo.pga_gateway_to_account_temp"
		,"pga_gateway_upload_category"=>"secure_pga.dbo.pga_gateway_upload_category"
		,"pga_groups"=>"secure_pga.dbo.pga_groups"
		,"pga_gta_history"=>"secure_pga.dbo.pga_gta_history"
		,"pga_health_boost"=>"secure_pga.dbo.pga_health_boost"
		,"pga_invoices"=>"secure_pga.dbo.pga_invoices"
		,"pga_invoice_batch"=>"secure_pga.dbo.pga_invoice_batch"
		,"pga_keys"=>"secure_pga.dbo.pga_keys"
		,"pga_keys_test"=>"secure_pga.dbo.pga_keys_test"
		,"pga_merchant_code"=>"secure_pga.dbo.pga_merchant_code"
		,"pga_merchant_promo_bin"=>"secure_pga.dbo.pga_merchant_promo_bin"
		,"pga_merchant_service_tax"=>"secure_pga.dbo.pga_merchant_service_tax"
		,"pga_merchant_service_tax_split"=>"secure_pga.dbo.pga_merchant_service_tax_split"
		,"pga_module_release_settings"=>"secure_pga.dbo.pga_module_release_settings"
		,"pga_module_release_settings_log"=>"secure_pga.dbo.pga_module_release_settings_log"
		,"pga_nodal_account"=>"secure_pga.dbo.pga_nodal_account"
		,"pga_notification"=>"secure_pga.dbo.pga_notification"
		,"pga_notification_format"=>"secure_pga.dbo.pga_notification_format"
		,"pga_notification_format_new"=>"secure_pga.dbo.pga_notification_format_new"
		,"pga_notification_format_wl"=>"secure_pga.dbo.pga_notification_format_wl"
		,"pga_notification_format_wl_temp"=>"secure_pga.dbo.pga_notification_format_wl_temp"
		,"pga_offline_bank_bins"=>"secure_pga.dbo.pga_offline_bank_bins"
		,"pga_offline_emi"=>"secure_pga.dbo.pga_offline_emi"
		,"pga_offline_emi_config"=>"secure_pga.dbo.pga_offline_emi_config"
		,"pga_offline_file_details"=>"secure_pga.dbo.pga_offline_file_details"
		,"pga_page_pref"=>"secure_pga.dbo.pga_page_pref"
		,"pga_page_template"=>"secure_pga.dbo.pga_page_template"
		,"pga_page_template_pref"=>"secure_pga.dbo.pga_page_template_pref"
		,"pga_payment_mode"=>"secure_pga.dbo.pga_payment_mode"
		,"pga_payment_phone"=>"secure_pga.dbo.pga_payment_phone"
		,"pga_paypal_access"=>"secure_pga.dbo.pga_paypal_access"
		,"pga_proactive_alert"=>"secure_pga.dbo.pga_proactive_alert"
		,"pga_processor"=>"secure_pga.dbo.pga_processor"
		,"pga_processor_commercial"=>"secure_pga.dbo.pga_processor_commercial"
		,"pga_processor_commercial_details"=>"secure_pga.dbo.pga_processor_commercial_details"
		,"pga_product"=>"secure_pga.dbo.pga_product"
		,"pga_product_account"=>"secure_pga.dbo.pga_product_account"
		,"pga_product_invoice"=>"secure_pga.dbo.pga_product_invoice"
		,"pga_product_invoice_item"=>"secure_pga.dbo.pga_product_invoice_item"
		,"pga_product_subscr"=>"secure_pga.dbo.pga_product_subscr"
		,"pga_product_subscr_history"=>"secure_pga.dbo.pga_product_subscr_history"
		,"pga_product_usage"=>"secure_pga.dbo.pga_product_usage"
		,"pga_product_usage_summary"=>"secure_pga.dbo.pga_product_usage_summary"
		,"pga_prosacc_priority_history"=>"secure_pga.dbo.pga_prosacc_priority_history"
		,"pga_prosacc_priority_mapping"=>"secure_pga.dbo.pga_prosacc_priority_mapping"
		,"pga_prosacc_priority_mapping_backup"=>"secure_pga.dbo.pga_prosacc_priority_mapping_backup"
		,"pga_refund_enabled_history"=>"secure_pga.dbo.pga_refund_enabled_history"
		,"pga_refund_file_upload"=>"secure_pga.dbo.pga_refund_file_upload"
		,"pga_refund_restriction"=>"secure_pga.dbo.pga_refund_restriction"
		,"pga_refund_restriction_history"=>"secure_pga.dbo.pga_refund_restriction_history"
		,"pga_refund_restriction_status_history"=>"secure_pga.dbo.pga_refund_restriction_status_history"
		,"pga_refund_upload_data"=>"secure_pga.dbo.pga_refund_upload_data"
		,"pga_report_group"=>"secure_pga.dbo.pga_report_group"
		,"pga_report_template"=>"secure_pga.dbo.pga_report_template"
		,"pga_report_type"=>"secure_pga.dbo.pga_report_type"
		,"pga_rmslite_account_settings"=>"secure_pga.dbo.pga_rmslite_account_settings"
		,"pga_rmslite_list"=>"secure_pga.dbo.pga_rmslite_list"
		,"pga_rmslite_settings"=>"secure_pga.dbo.pga_rmslite_settings"
		,"pga_rms_fee_setting"=>"secure_pga.dbo.pga_rms_fee_setting"
		,"pga_rms_md5cc"=>"secure_pga.dbo.pga_rms_md5cc"
		,"pga_role_report"=>"secure_pga.dbo.pga_role_report"
		,"pga_salt"=>"secure_pga.dbo.pga_salt"
		,"pga_service_tax"=>"secure_pga.dbo.pga_service_tax"
		,"pga_service_tax_split"=>"secure_pga.dbo.pga_service_tax_split"
		,"pga_setting"=>"secure_pga.dbo.pga_setting"
		,"pga_settlement_history"=>"secure_pga.dbo.pga_settlement_history"
		,"pga_smrt_master"=>"secure_pga.dbo.pga_smrt_master"
		,"pga_smrt_ranking"=>"secure_pga.dbo.pga_smrt_ranking"
		,"pga_smspay"=>"secure_pga.dbo.pga_smspay"
		,"pga_split_refund_data"=>"secure_pga.dbo.pga_split_refund_data"
		,"pga_status_post_log"=>"secure_pga.dbo.pga_status_post_log"
		,"pga_surcharge_to_account"=>"secure_pga.dbo.pga_surcharge_to_account"
		,"pga_temp_processor_health"=>"secure_pga.dbo.pga_temp_processor_health"
		,"pga_ticker_message"=>"secure_pga.dbo.pga_ticker_message"
		,"pga_tp_gateway_mapping"=>"secure_pga.dbo.pga_tp_gateway_mapping"
		,"pga_transaction"=>"secure_pga.dbo.pga_transaction"
		,"pga_transaction_batch"=>"secure_pga.dbo.pga_transaction_batch"
		,"pga_txnupdate_history"=>"secure_pga.dbo.pga_txnupdate_history"
		,"pga_txn_password"=>"secure_pga.dbo.pga_txn_password"
		,"pga_user_account"=>"secure_pga.dbo.pga_user_account"
		,"pga_user_password"=>"secure_pga.dbo.pga_user_password"
		,"pga_v2_merchants"=>"secure_pga.dbo.pga_v2_merchants"
		,"pga_white_label_admin"=>"secure_pga.dbo.pga_white_label_admin"
		,"pga_wl_creation"=>"secure_pga.dbo.pga_wl_creation"
		,"sgl_block"=>"secure_pga.dbo.sgl_block"
		,"sgl_block_assignment"=>"secure_pga.dbo.sgl_block_assignment"
		,"sgl_block_role"=>"secure_pga.dbo.sgl_block_role"
		,"sgl_email_queue"=>"secure_pga.dbo.sgl_email_queue"
		,"sgl_group"=>"secure_pga.dbo.sgl_group"
		,"sgl_group_permission"=>"secure_pga.dbo.sgl_group_permission"
		,"sgl_login"=>"secure_pga.dbo.sgl_login"
		,"sgl_log_table"=>"secure_pga.dbo.sgl_log_table"
		,"sgl_module"=>"secure_pga.dbo.sgl_module"
		,"sgl_organisation"=>"secure_pga.dbo.sgl_organisation"
		,"sgl_organisation_type"=>"secure_pga.dbo.sgl_organisation_type"
		,"sgl_org_preference"=>"secure_pga.dbo.sgl_org_preference"
		,"sgl_permission"=>"secure_pga.dbo.sgl_permission"
		,"sgl_preference"=>"secure_pga.dbo.sgl_preference"
		,"sgl_role"=>"secure_pga.dbo.sgl_role"
		,"sgl_role_permission"=>"secure_pga.dbo.sgl_role_permission"
		,"sgl_section"=>"secure_pga.dbo.sgl_section"
		,"sgl_sequence"=>"secure_pga.dbo.sgl_sequence"
		,"sgl_table_lock"=>"secure_pga.dbo.sgl_table_lock"
		,"sgl_uri_alias"=>"secure_pga.dbo.sgl_uri_alias"
		,"sgl_user_cookie"=>"secure_pga.dbo.sgl_user_cookie"
		,"sgl_user_permission"=>"secure_pga.dbo.sgl_user_permission"
		,"sgl_user_preference"=>"secure_pga.dbo.sgl_user_preference"
		,"sgl_user_session"=>"secure_pga.dbo.sgl_user_session"
		,"sgl_usr"=>"secure_pga.dbo.sgl_usr"
		,"tkr_config"=>"secure_pga.dbo.tkr_config"
		,"tkr_data_key"=>"secure_pga.dbo.tkr_data_key"
		,"tkr_merchant"=>"secure_pga.dbo.tkr_merchant"
		,"tkr_rest_key"=>"secure_pga.dbo.tkr_rest_key"
		,"tkr_rest_key1"=>"secure_pga.dbo.tkr_rest_key1"
		,"tkr_token"=>"secure_pga.dbo.tkr_token"	
	
		,"pga_account_broking_settlement"=>"secure_pgatxn.dbo.pga_account_broking_settlement"
		,"pga_account_settlement"=>"secure_pgatxn.dbo.pga_account_settlement"
		,"pga_account_settlement_updated"=>"secure_pgatxn.dbo.pga_account_settlement_updated"
		,"pga_account_settlement_utr"=>"secure_pgatxn.dbo.pga_account_settlement_utr"
		,"pga_account_settlement_utr_txn"=>"secure_pgatxn.dbo.pga_account_settlement_utr_txn"
		,"pga_amc_transactions"=>"secure_pgatxn.dbo.pga_amc_transactions"
		,"pga_auto_pay_transactions"=>"secure_pgatxn.dbo.pga_auto_pay_transactions"
		,"pga_broking_payment"=>"secure_pgatxn.dbo.pga_broking_payment"
		,"pga_broking_settlement"=>"secure_pgatxn.dbo.pga_broking_settlement"
		,"pga_case"=>"secure_pgatxn.dbo.pga_case"
		,"pga_case_attachement"=>"secure_pgatxn.dbo.pga_case_attachement"
		,"pga_case_chargeback"=>"secure_pgatxn.dbo.pga_case_chargeback"
		,"pga_case_flaggedpayments"=>"secure_pgatxn.dbo.pga_case_flaggedpayments"
		,"pga_case_history"=>"secure_pgatxn.dbo.pga_case_history"
		,"pga_case_reminders"=>"secure_pgatxn.dbo.pga_case_reminders"
		,"pga_case_target_date_history"=>"secure_pgatxn.dbo.pga_case_target_date_history"
		,"pga_challan_bank_details"=>"secure_pgatxn.dbo.pga_challan_bank_details"
		,"pga_challan_details"=>"secure_pgatxn.dbo.pga_challan_details"
		,"pga_challan_reconcile"=>"secure_pgatxn.dbo.pga_challan_reconcile"
		,"pga_challan_transaction"=>"secure_pgatxn.dbo.pga_challan_transaction"
		,"pga_citi_arn_txn"=>"secure_pgatxn.dbo.pga_citi_arn_txn"
		,"pga_citi_arn_upload"=>"secure_pgatxn.dbo.pga_citi_arn_upload"
		,"pga_citi_reconcile_txn"=>"secure_pgatxn.dbo.pga_citi_reconcile_txn"
		,"pga_cron_log"=>"secure_pgatxn.dbo.pga_cron_log"
		,"pga_currency_log"=>"secure_pgatxn.dbo.pga_currency_log"
		,"pga_customer_transaction"=>"secure_pgatxn.dbo.pga_customer_transaction"
		,"pga_cust_payment"=>"secure_pgatxn.dbo.pga_cust_payment"
		,"pga_cust_payment_cc"=>"secure_pgatxn.dbo.pga_cust_payment_cc"
		,"pga_cust_payment_detail"=>"secure_pgatxn.dbo.pga_cust_payment_detail"
		,"pga_cust_payment_products"=>"secure_pgatxn.dbo.pga_cust_payment_products"
		,"pga_emi_failed_txn"=>"secure_pgatxn.dbo.pga_emi_failed_txn"
		,"pga_emi_transaction"=>"secure_pgatxn.dbo.pga_emi_transaction"
		,"pga_failure_reason"=>"secure_pgatxn.dbo.pga_failure_reason"
		,"pga_failure_to_success_log"=>"secure_pgatxn.dbo.pga_failure_to_success_log"
		,"pga_files"=>"secure_pgatxn.dbo.pga_files"
		,"pga_incomplete_batch_txns"=>"secure_pgatxn.dbo.pga_incomplete_batch_txns"
		,"pga_incomplete_txn_log"=>"secure_pgatxn.dbo.pga_incomplete_txn_log"
		,"pga_invoice_details"=>"secure_pgatxn.dbo.pga_invoice_details"
		,"pga_invoice_history"=>"secure_pgatxn.dbo.pga_invoice_history"
		,"pga_offline_emi_transaction"=>"secure_pgatxn.dbo.pga_offline_emi_transaction"
		,"pga_payment_phone"=>"secure_pgatxn.dbo.pga_payment_phone"
		,"pga_paypal_ipn"=>"secure_pgatxn.dbo.pga_paypal_ipn"
		,"pga_reconcile_batch"=>"secure_pgatxn.dbo.pga_reconcile_batch"
		,"pga_reconcile_batch_txn"=>"secure_pgatxn.dbo.pga_reconcile_batch_txn"
		,"pga_reconcile_refund"=>"secure_pgatxn.dbo.pga_reconcile_refund"
		,"pga_reconcile_txn"=>"secure_pgatxn.dbo.pga_reconcile_txn"
		,"pga_reconcile_upload"=>"secure_pgatxn.dbo.pga_reconcile_upload"
		,"pga_refund_batches"=>"secure_pgatxn.dbo.pga_refund_batches"
		,"pga_refund_batch_txns"=>"secure_pgatxn.dbo.pga_refund_batch_txns"
		,"pga_request"=>"secure_pgatxn.dbo.pga_request"
		,"pga_request_status_history"=>"secure_pgatxn.dbo.pga_request_status_history"
		,"pga_response"=>"secure_pgatxn.dbo.pga_response"
		,"pga_reversal_response"=>"secure_pgatxn.dbo.pga_reversal_response"
		,"pga_review"=>"secure_pgatxn.dbo.pga_review"
		,"pga_rms_card"=>"secure_pgatxn.dbo.pga_rms_card"
		,"pga_rms_transaction_tdr"=>"secure_pgatxn.dbo.pga_rms_transaction_tdr"
		,"pga_rupay_transactions"=>"secure_pgatxn.dbo.pga_rupay_transactions"
		,"pga_settlement"=>"secure_pgatxn.dbo.pga_settlement"
		,"pga_settlement_stopped"=>"secure_pgatxn.dbo.pga_settlement_stopped"
		,"pga_settlement_txn"=>"secure_pgatxn.dbo.pga_settlement_txn"
		,"pga_settlement_v2"=>"secure_pgatxn.dbo.pga_settlement_v2"
		,"pga_split_params"=>"secure_pgatxn.dbo.pga_split_params"
		,"pga_split_params1"=>"secure_pgatxn.dbo.pga_split_params1"
		,"pga_status_post_log"=>"secure_pgatxn.dbo.pga_status_post_log"
		,"pga_subscription"=>"secure_pgatxn.dbo.pga_subscription"
		,"pga_subscription_dues"=>"secure_pgatxn.dbo.pga_subscription_dues"
		,"pga_subscription_request"=>"secure_pgatxn.dbo.pga_subscription_request"
		,"pga_subscription_status_history"=>"secure_pgatxn.dbo.pga_subscription_status_history"
		,"pga_tmp_bank_tdr"=>"secure_pgatxn.dbo.pga_tmp_bank_tdr"
		,"pga_transaction"=>"secure_pgatxn.dbo.pga_transaction"
		,"pga_transaction_attempt"=>"secure_pgatxn.dbo.pga_transaction_attempt"
		,"pga_transaction_service_tax"=>"secure_pgatxn.dbo.pga_transaction_service_tax"
		,"pga_transaction_tdr"=>"secure_pgatxn.dbo.pga_transaction_tdr"
		,"pga_transaction_temp_settle"=>"secure_pgatxn.dbo.pga_transaction_temp_settle"
		,"pga_txn_session"=>"secure_pgatxn.dbo.pga_txn_session"
		,"pga_upi_transaction"=>"secure_pgatxn.dbo.pga_upi_transaction"
		,"pga_v2_payments"=>"secure_pgatxn.dbo.pga_v2_payments"
	
		,"blocks"=>"secure_lib.dbo.blocks"
		,"card_bin"=>"secure_lib.dbo.card_bin"
		,"card_country_bin"=>"secure_lib.dbo.card_country_bin"
		,"countries"=>"secure_lib.dbo.countries"
		,"issuer_details"=>"secure_lib.dbo.issuer_details"
		,"locations"=>"secure_lib.dbo.locations"
		,"tblbank"=>"secure_lib.dbo.tblbank"
		,"tblbin"=>"secure_lib.dbo.tblbin"
		,"tidf_bin"=>"secure_lib.dbo.tidf_bin"
		,"tissuing"=>"secure_lib.dbo.tissuing"
	);
	  
	  
	  $tableNamesWithPrefix = array();
	  foreach($tableList as $tableName) {
		$tableNamesWithPrefix[$tableName] = $tableArray[$tableName];
	  }
	  
	  return $tableNamesWithPrefix;
	}
	
	
	
	function getSubStr($query, $fromStr, $toStr) {
		$fromStrLen = strlen($fromStr);
		$startingPos = stripos($query, $fromStr) + $fromStrLen;
		$endingPos = stripos($query, $toStr);
		if(empty($endingPos))
		{
		return substr($query, $startingPos);	
		}
		else{
			return substr($query, $startingPos, ($endingPos - $startingPos));
		}
	}


	function removeAliseName($tablesName) {
		$cleanTablesName = array();
		foreach($tablesName as $tableName) {
			$temp = explode(" ",$tableName);
			$cleanTablesName[] = trim($temp[0],"[]");
		}
		return $cleanTablesName;
	}
	
	function strReplaceTableName($query, $tableNames){
		foreach($tableNames as $tableNameBare => $tableNameWithPrefix) {
			if(strlen($tableNameBare)>3){
			$patterns[]='#\b'.$tableNameBare.'\b#iU';
			$replacements[]=$tableNameWithPrefix.'';
			}
		}
		$query = preg_replace($patterns, $replacements, $query);
		return $query;
	}
	
    // }}}
    // {{{ nextResult()

    /**
     * Move the internal mssql result pointer to the next available result
     *
     * @param a valid fbsql result resource
     *
     * @access public
     *
     * @return true if a result is available otherwise return false
     */
    function nextResult($result)
    {
        return @mssql_next_result($result);
    }

    // }}}
    // {{{ fetchInto()

    /**
     * Places a row from the result set into the given array
     *
     * Formating of the array and the data therein are configurable.
     * See DB_result::fetchInto() for more information.
     *
     * This method is not meant to be called directly.  Use
     * DB_result::fetchInto() instead.  It can't be declared "protected"
     * because DB_result is a separate object.
     *
     * @param resource $result    the query result resource
     * @param array    $arr       the referenced array to put the data in
     * @param int      $fetchmode how the resulting array should be indexed
     * @param int      $rownum    the row number to fetch (0 = first row)
     *
     * @return mixed  DB_OK on success, NULL when the end of a result set is
     *                 reached or on failure
     *
     * @see DB_result::fetchInto()
     */
    function fetchInto($result, &$arr, $fetchmode, $rownum = null)
    {
        if ($rownum !== null) {
            if (!@mssql_data_seek($result, $rownum)) {
                return null;
            }
        }
        if ($fetchmode & DB_FETCHMODE_ASSOC) {
            $arr = @mssql_fetch_assoc($result);
            if ($this->options['portability'] & DB_PORTABILITY_LOWERCASE && $arr) {
                $arr = array_change_key_case($arr, CASE_LOWER);
            }
        } else {
            $arr = @mssql_fetch_row($result);
        }
        if (!$arr) {
            return null;
        }
        if ($this->options['portability'] & DB_PORTABILITY_RTRIM) {
            $this->_rtrimArrayValues($arr);
        }
        if ($this->options['portability'] & DB_PORTABILITY_NULL_TO_EMPTY) {
            $this->_convertNullArrayValuesToEmpty($arr);
        }
        return DB_OK;
    }

    // }}}
    // {{{ freeResult()

    /**
     * Deletes the result set and frees the memory occupied by the result set
     *
     * This method is not meant to be called directly.  Use
     * DB_result::free() instead.  It can't be declared "protected"
     * because DB_result is a separate object.
     *
     * @param resource $result  PHP's query result resource
     *
     * @return bool  TRUE on success, FALSE if $result is invalid
     *
     * @see DB_result::free()
     */
    function freeResult($result)
    {
        return is_resource($result) ? mssql_free_result($result) : false;
    }

    // }}}
    // {{{ numCols()

    /**
     * Gets the number of columns in a result set
     *
     * This method is not meant to be called directly.  Use
     * DB_result::numCols() instead.  It can't be declared "protected"
     * because DB_result is a separate object.
     *
     * @param resource $result  PHP's query result resource
     *
     * @return int  the number of columns.  A DB_Error object on failure.
     *
     * @see DB_result::numCols()
     */
    function numCols($result)
    {
        $cols = @mssql_num_fields($result);
        if (!$cols) {
            return $this->mssqlRaiseError();
        }
        return $cols;
    }

    // }}}
    // {{{ numRows()

    /**
     * Gets the number of rows in a result set
     *
     * This method is not meant to be called directly.  Use
     * DB_result::numRows() instead.  It can't be declared "protected"
     * because DB_result is a separate object.
     *
     * @param resource $result  PHP's query result resource
     *
     * @return int  the number of rows.  A DB_Error object on failure.
     *
     * @see DB_result::numRows()
     */
    function numRows($result)
    {
        $rows = @mssql_num_rows($result);
        if ($rows === false) {
            return $this->mssqlRaiseError();
        }
        return $rows;
    }

    // }}}
    // {{{ autoCommit()

    /**
     * Enables or disables automatic commits
     *
     * @param bool $onoff  true turns it on, false turns it off
     *
     * @return int  DB_OK on success.  A DB_Error object if the driver
     *               doesn't support auto-committing transactions.
     */
    function autoCommit($onoff = false)
    {
        // XXX if $this->transaction_opcount > 0, we should probably
        // issue a warning here.
        $this->autocommit = $onoff ? true : false;
        return DB_OK;
    }

    // }}}
    // {{{ commit()

    /**
     * Commits the current transaction
     *
     * @return int  DB_OK on success.  A DB_Error object on failure.
     */
    function commit()
    {
        if ($this->transaction_opcount > 0) {
            if (!@mssql_select_db($this->_db, $this->connection)) {
                return $this->mssqlRaiseError(DB_ERROR_NODBSELECTED);
            }
            $result = @mssql_query('COMMIT TRAN', $this->connection);
            $this->transaction_opcount = 0;
            if (!$result) {
                return $this->mssqlRaiseError();
            }
        }
        return DB_OK;
    }

    // }}}
    // {{{ rollback()

    /**
     * Reverts the current transaction
     *
     * @return int  DB_OK on success.  A DB_Error object on failure.
     */
    function rollback()
    {
        if ($this->transaction_opcount > 0) {
            if (!@mssql_select_db($this->_db, $this->connection)) {
                return $this->mssqlRaiseError(DB_ERROR_NODBSELECTED);
            }
            $result = @mssql_query('ROLLBACK TRAN', $this->connection);
            $this->transaction_opcount = 0;
            if (!$result) {
                return $this->mssqlRaiseError();
            }
        }
        return DB_OK;
    }

    // }}}
    // {{{ affectedRows()

    /**
     * Determines the number of rows affected by a data maniuplation query
     *
     * 0 is returned for queries that don't manipulate data.
     *
     * @return int  the number of rows.  A DB_Error object on failure.
     */
    function affectedRows()
    {
        if ($this->_last_query_manip) {
            $res = @mssql_query('select @@rowcount', $this->connection);
            if (!$res) {
                return $this->mssqlRaiseError();
            }
            $ar = @mssql_fetch_row($res);
            if (!$ar) {
                $result = 0;
            } else {
                @mssql_free_result($res);
                $result = $ar[0];
            }
        } else {
            $result = 0;
        }
        return $result;
    }

    // }}}
    // {{{ nextId()

    /**
     * Returns the next free id in a sequence
     *
     * @param string  $seq_name  name of the sequence
     * @param boolean $ondemand  when true, the seqence is automatically
     *                            created if it does not exist
     *
     * @return int  the next id number in the sequence.
     *               A DB_Error object on failure.
     *
     * @see DB_common::nextID(), DB_common::getSequenceName(),
     *      DB_mssql::createSequence(), DB_mssql::dropSequence()
     */
    function nextId($seq_name, $ondemand = true)
    {
        $seqname = $this->getSequenceName($seq_name);
        if (!@mssql_select_db($this->_db, $this->connection)) {
            return $this->mssqlRaiseError(DB_ERROR_NODBSELECTED);
        }
        $repeat = 0;
        do {
            $this->pushErrorHandling(PEAR_ERROR_RETURN);
            $result = $this->query("INSERT INTO $seqname (vapor) VALUES (0)");
            $this->popErrorHandling();
            if ($ondemand && DB::isError($result) &&
                ($result->getCode() == DB_ERROR || $result->getCode() == DB_ERROR_NOSUCHTABLE))
            {
                $repeat = 1;
                $result = $this->createSequence($seq_name);
                if (DB::isError($result)) {
                    return $this->raiseError($result);
                }
            } elseif (!DB::isError($result)) {
                $result =& $this->query("SELECT IDENT_CURRENT('$seqname')");
                if (DB::isError($result)) {
                    /* Fallback code for MS SQL Server 7.0, which doesn't have
                     * IDENT_CURRENT. This is *not* safe for concurrent
                     * requests, and really, if you're using it, you're in a
                     * world of hurt. Nevertheless, it's here to ensure BC. See
                     * bug #181 for the gory details.*/
                    $result =& $this->query("SELECT @@IDENTITY FROM $seqname");
                }
                $repeat = 0;
            } else {
                $repeat = false;
            }
        } while ($repeat);
        if (DB::isError($result)) {
            return $this->raiseError($result);
        }
        $result = $result->fetchRow(DB_FETCHMODE_ORDERED);
        return $result[0];
    }

    /**
     * Creates a new sequence
     *
     * @param string $seq_name  name of the new sequence
     *
     * @return int  DB_OK on success.  A DB_Error object on failure.
     *
     * @see DB_common::createSequence(), DB_common::getSequenceName(),
     *      DB_mssql::nextID(), DB_mssql::dropSequence()
     */
    function createSequence($seq_name)
    {
        return $this->query('CREATE TABLE '
                            . $this->getSequenceName($seq_name)
                            . ' ([id] [int] IDENTITY (1, 1) NOT NULL,'
                            . ' [vapor] [int] NULL)');
    }

    // }}}
    // {{{ dropSequence()

    /**
     * Deletes a sequence
     *
     * @param string $seq_name  name of the sequence to be deleted
     *
     * @return int  DB_OK on success.  A DB_Error object on failure.
     *
     * @see DB_common::dropSequence(), DB_common::getSequenceName(),
     *      DB_mssql::nextID(), DB_mssql::createSequence()
     */
    function dropSequence($seq_name)
    {
        return $this->query('DROP TABLE ' . $this->getSequenceName($seq_name));
    }

    // }}}
    // {{{ quoteIdentifier()

    /**
     * Quotes a string so it can be safely used as a table or column name
     *
     * @param string $str  identifier name to be quoted
     *
     * @return string  quoted identifier string
     *
     * @see DB_common::quoteIdentifier()
     * @since Method available since Release 1.6.0
     */
    function quoteIdentifier($str)
    {
        return '[' . str_replace(']', ']]', $str) . ']';
    }

    // }}}
    // {{{ mssqlRaiseError()

    /**
     * Produces a DB_Error object regarding the current problem
     *
     * @param int $errno  if the error is being manually raised pass a
     *                     DB_ERROR* constant here.  If this isn't passed
     *                     the error information gathered from the DBMS.
     *
     * @return object  the DB_Error object
     *
     * @see DB_common::raiseError(),
     *      DB_mssql::errorNative(), DB_mssql::errorCode()
     */
    function mssqlRaiseError($code = null)
    {
        $message = @mssql_get_last_message();
        if (!$code) {
            $code = $this->errorNative();
        }
        return $this->raiseError($this->errorCode($code, $message),
                                 null, null, null, "$code - $message");
    }

    // }}}
    // {{{ errorNative()

    /**
     * Gets the DBMS' native error code produced by the last query
     *
     * @return int  the DBMS' error code
     */
    function errorNative()
    {
        $res = @mssql_query('select @@ERROR as ErrorCode', $this->connection);
        if (!$res) {
            return DB_ERROR;
        }
        $row = @mssql_fetch_row($res);
        return $row[0];
    }

    // }}}
    // {{{ errorCode()

    /**
     * Determines PEAR::DB error code from mssql's native codes.
     *
     * If <var>$nativecode</var> isn't known yet, it will be looked up.
     *
     * @param  mixed  $nativecode  mssql error code, if known
     * @return integer  an error number from a DB error constant
     * @see errorNative()
     */
    function errorCode($nativecode = null, $msg = '')
    {
        if (!$nativecode) {
            $nativecode = $this->errorNative();
        }
        if (isset($this->errorcode_map[$nativecode])) {
            if ($nativecode == 3701
                && preg_match('/Cannot drop the index/i', $msg))
            {
                return DB_ERROR_NOT_FOUND;
            }
            return $this->errorcode_map[$nativecode];
        } else {
            return DB_ERROR;
        }
    }

    // }}}
    // {{{ tableInfo()

    /**
     * Returns information about a table or a result set
     *
     * NOTE: only supports 'table' and 'flags' if <var>$result</var>
     * is a table name.
     *
     * @param object|string  $result  DB_result object from a query or a
     *                                 string containing the name of a table.
     *                                 While this also accepts a query result
     *                                 resource identifier, this behavior is
     *                                 deprecated.
     * @param int            $mode    a valid tableInfo mode
     *
     * @return array  an associative array with the information requested.
     *                 A DB_Error object on failure.
     *
     * @see DB_common::tableInfo()
     */
    function tableInfo($result, $mode = null)
    {
        if (is_string($result)) {
            /*
             * Probably received a table name.
             * Create a result resource identifier.
             */
            if (!@mssql_select_db($this->_db, $this->connection)) {
                return $this->mssqlRaiseError(DB_ERROR_NODBSELECTED);
            }
            $id = @mssql_query("SELECT * FROM $result WHERE 1=0",
                               $this->connection);
            $got_string = true;
        } elseif (isset($result->result)) {
            /*
             * Probably received a result object.
             * Extract the result resource identifier.
             */
            $id = $result->result;
            $got_string = false;
        } else {
            /*
             * Probably received a result resource identifier.
             * Copy it.
             * Deprecated.  Here for compatibility only.
             */
            $id = $result;
            $got_string = false;
        }

        if (!is_resource($id)) {
            return $this->mssqlRaiseError(DB_ERROR_NEED_MORE_DATA);
        }

        if ($this->options['portability'] & DB_PORTABILITY_LOWERCASE) {
            $case_func = 'strtolower';
        } else {
            $case_func = 'strval';
        }

        $count = @mssql_num_fields($id);
        $res   = array();

        if ($mode) {
            $res['num_fields'] = $count;
        }

        for ($i = 0; $i < $count; $i++) {
            if ($got_string) {
                $flags = $this->_mssql_field_flags($result,
                        @mssql_field_name($id, $i));
                if (DB::isError($flags)) {
                    return $flags;
                }
            } else {
                $flags = '';
            }

            $res[$i] = array(
                'table' => $got_string ? $case_func($result) : '',
                'name'  => $case_func(@mssql_field_name($id, $i)),
                'type'  => @mssql_field_type($id, $i),
                'len'   => @mssql_field_length($id, $i),
                'flags' => $flags,
            );
            if ($mode & DB_TABLEINFO_ORDER) {
                $res['order'][$res[$i]['name']] = $i;
            }
            if ($mode & DB_TABLEINFO_ORDERTABLE) {
                $res['ordertable'][$res[$i]['table']][$res[$i]['name']] = $i;
            }
        }

        // free the result only if we were called on a table
        if ($got_string) {
            @mssql_free_result($id);
        }
        return $res;
    }

    // }}}
    // {{{ _mssql_field_flags()

    /**
     * Get a column's flags
     *
     * Supports "not_null", "primary_key",
     * "auto_increment" (mssql identity), "timestamp" (mssql timestamp),
     * "unique_key" (mssql unique index, unique check or primary_key) and
     * "multiple_key" (multikey index)
     *
     * mssql timestamp is NOT similar to the mysql timestamp so this is maybe
     * not useful at all - is the behaviour of mysql_field_flags that primary
     * keys are alway unique? is the interpretation of multiple_key correct?
     *
     * @param string $table   the table name
     * @param string $column  the field name
     *
     * @return string  the flags
     *
     * @access private
     * @author Joern Barthel <j_barthel@web.de>
     */
    function _mssql_field_flags($table, $column)
    {
        static $tableName = null;
        static $flags = array();

        if ($table != $tableName) {

            $flags = array();
            $tableName = $table;

            // get unique and primary keys
            $res = $this->getAll("EXEC SP_HELPINDEX $table", DB_FETCHMODE_ASSOC);
            if (DB::isError($res)) {
                return $res;
            }

            foreach ($res as $val) {
                $keys = explode(', ', $val['index_keys']);

                if (sizeof($keys) > 1) {
                    foreach ($keys as $key) {
                        $this->_add_flag($flags[$key], 'multiple_key');
                    }
                }

                if (strpos($val['index_description'], 'primary key')) {
                    foreach ($keys as $key) {
                        $this->_add_flag($flags[$key], 'primary_key');
                    }
                } elseif (strpos($val['index_description'], 'unique')) {
                    foreach ($keys as $key) {
                        $this->_add_flag($flags[$key], 'unique_key');
                    }
                }
            }

            // get auto_increment, not_null and timestamp
            $res = $this->getAll("EXEC SP_COLUMNS $table", DB_FETCHMODE_ASSOC);
            if (DB::isError($res)) {
                return $res;
            }

            foreach ($res as $val) {
                $val = array_change_key_case($val, CASE_LOWER);
                if ($val['nullable'] == '0') {
                    $this->_add_flag($flags[$val['column_name']], 'not_null');
                }
                if (strpos($val['type_name'], 'identity')) {
                    $this->_add_flag($flags[$val['column_name']], 'auto_increment');
                }
                if (strpos($val['type_name'], 'timestamp')) {
                    $this->_add_flag($flags[$val['column_name']], 'timestamp');
                }
            }
        }

        if (array_key_exists($column, $flags)) {
            return(implode(' ', $flags[$column]));
        }
        return '';
    }

    // }}}
    // {{{ _add_flag()

    /**
     * Adds a string to the flags array if the flag is not yet in there
     * - if there is no flag present the array is created
     *
     * @param array  &$array  the reference to the flag-array
     * @param string $value   the flag value
     *
     * @return void
     *
     * @access private
     * @author Joern Barthel <j_barthel@web.de>
     */
    function _add_flag(&$array, $value)
    {
        if (!is_array($array)) {
            $array = array($value);
        } elseif (!in_array($value, $array)) {
            array_push($array, $value);
        }
    }

    // }}}
    // {{{ getSpecialQuery()

    /**
     * Obtains the query string needed for listing a given type of objects
     *
     * @param string $type  the kind of objects you want to retrieve
     *
     * @return string  the SQL query string or null if the driver doesn't
     *                  support the object type requested
     *
     * @access protected
     * @see DB_common::getListOf()
     */
    function getSpecialQuery($type)
    {
        switch ($type) {
            case 'tables':
                return "SELECT name FROM sysobjects WHERE type = 'U'"
                       . ' ORDER BY name';
            case 'views':
                return "SELECT name FROM sysobjects WHERE type = 'V'";
            default:
                return null;
        }
    }

    // }}}
}

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 */

?>
