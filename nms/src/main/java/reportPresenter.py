#!/usr/local/bin/python
# 
# Report Presenter
# ================
# Author    : Deepak
# Copyright : HSBSoft Technologies Pvt. Ltd
# =========================================
"""
****************************************************************************************************
===============
ReportPresenter
===============
"""
from unmsutil import *
from international import *
from uwebcom import *
from uwebreport import *
import basePresenter
import reportparams
import interpolator
import extrapolator
import copy
import math
import maxInputStats
import json
import collections
import time, calendar
import os, datetime

ONE_MINUTE = 60
ONE_HOUR = 3600
ONE_DAY = 86400
SIXTY_MINUTES = 60
FIVE_MINUTES_IN_HOUR = 12
TWENTY_FOUR_HOURS = 24
SEVEN_DAYS = 7
THIRTY_DAYS = 30
THIRTY_ONE_DAYS = 31
MAXIMUM_ENTRIES = 24
HISTORY_COUNT = 10

LARGE_REPORT_WIDTH = 1130
DEFAULT_REPORT_WIDTH = 845
SMALL_REPORT_WIDTH = 420

PATH_SEP = '/'
ROOT_PATH = 'root'

class NoReportException(Exception):
	pass

class NoDefaultReportProfileConfigured(Exception):
	pass

class CallNetflowReportException(Exception):
	pass

class ReportPresenter(basePresenter.BasePresenter):
	"""Responsible for Report Page. Derived from BasePresenter.
	"""
	single_name = 'Report'
	plural_name = 'Reports'
	url_suffix = 'report'
	module_name = 'Report'
	dblog_module = 'Report'
	calendar_xpos = 12
	calendar_ypos = 167
	defaultTimeScale = 1
	LOG_SCHEMA.append(module_name)
	# For Simulation
	simulation_start_time = maxInputStats.reportSimulationStartTime
	simulation_end_time = maxInputStats.reportSimulationEndTime
	
	def __init__(self, report_profile, stat_server, resource_manager, licserver, 
			location_manager, domain_manager, left_panel, businessHrServer):
		"""Constructor of the class. Initialize the necessary variables.
		"""
		basePresenter.BasePresenter.__init__(self, licserver, resource_manager, location_manager, 
				domain_manager, left_panel)
		self.report_profile = report_profile
		self.stat_server = stat_server
		self.businessHrServer = businessHrServer
		self.unms = getUNMSPointer()
		#sql_stmt = 'SELECT * FROM tblStatMap'
		sql_stmt = queryStandardizer.getQuery('Q655')
		self.event_db = resource_manager.alarm_server.event_db
		objs = DatabaseServer.executeSQLStatement(sql_stmt)
		self.statname_id_map = {}
		self.statid_name_map = {}
		self.rawdn_id_map = {}
		self.statid_dn_map = {}
		self.statid_unit_map = {}
		self.time_scale = getReportParams().time_scale
		self.updateStatMapObjs()
		if objs != [] and objs != -1:
			map(lambda a, b = self.statname_id_map: b.update({a.get('name', ''): safe_int(a.get('statid'))}), objs)
			map(lambda a, b = self.statid_name_map: b.update({safe_int(a.get('statid')): a.get('name', '')}), objs)
			map(lambda a, b = self.rawdn_id_map: b.update({a.get('rawdn', ''): safe_int(a.get('statid'))}), objs)
			map(lambda a, b = self.statid_dn_map: b.update({safe_int(a.get('statid')): a.get('rawdn', '')}), objs)
			map(lambda a, b = self.statid_unit_map: b.update({safe_int(a.get('statid', '')): a.get('unit')}), objs)
		self.db_map = {
						'raw': self.stat_server.rawdb,
						'hour': self.stat_server.hourdb,
						'day': self.stat_server.daydb,
						'month': self.stat_server.monthdb,
					}
		self.event_db = self.resource_manager.alarm_server.event_db
		self.trap_db = self.resource_manager.alarm_server.trap_db
		# 15 minutes time interval for the default interpolation
		self.interpolate_time_range = 900
		self.extrapolator = extrapolator.Extrapolator(self.resource_manager, self.statid_dn_map, NULL_VALUE)
		self.interpolator = interpolator.Interpolator(self.interpolate_time_range, NULL_VALUE)
		# default constants
		self.group_id_map = {0: 'statid', 1: 'resid', 2: 'unit', 3: 'resid_statid'}
		self.stacked_charts = ['area', 'bar', 'stack_bar_time', 'percentile_area', 'bar_time', 'stack_bar', 'zarea', 'sq_bar_time', 'area_small', 'bar_small']
		#Added For FCL customization
		self.data_table_charts = ['data']
		self.node_summary_tables = []
		self.node_summary_details = []
		#Added For FCL customization
		self.pie_charts = ['pie', 'donut','dpie','rdonut','pyramid']
		self.threshold_based_charts = ['distribution', 'stack_bar_distribution', 'stack_bar_time_distribution']
		self.units_to_be_formatted = ['bps', 'bytes', '*bytes', '*Bytes', 'Bytes', 'Watts', 'pkts/sec', 'Kbps', 'Mbps', 'sec', '**bytes']
		self.history_count = HISTORY_COUNT	# default 10 (hrs, days, weeks, months)
		self.table_types_map = {'data': self.makeDataTable, 
					'summary': self.makeSummaryTable, 
					'events': self.makeEventsTable,
					'statewise_summary':self.makeVsatStatewiseTable,
					'downtime_report':self.makeDownTimeReport,
					'device_downtime_summary_report':self.makeDeviceDownTimeSummaryReport,
					'uptime_distribution':self.makeUptimeDistribution,
					'sla_summary_report': self.makeSLASummaryReport,
					'sla_detail_report': self.makeSLADetailReport,
					#'events_summary': self.makeEventsSummaryForCSV,
					'nodes_summary': self.makeNodeBasedSummaryTable, 
					'traps': self.makeTrapsTable, 
					'traps_summary':self.makeTrapsSummaryTable,
					'backhaul':self.makeBackhaulLinkUtilTable, 
					'backhaul_monthly':self.makeBackhaulMonthlyTable, 
					'uptime_summary':self.makeUptimeSummaryTable,
					'nw_statistic':self.makeVSATNetworkStatisticTable,
					'nw_efficiency':self.makeVSATNetworkEfficiencyTable,
					'vsatpingreport':self.makeVSATPingTable,
					}
		# url mapping
		self.NON_C3_REPORT_TYPES = ['events_summary','pattern','stack_bar']
		self.URLMap[self.setDefault] = 'setdefault%s' %(self.url_suffix)
		self.URLMap[self.modifyGroupReport] = 'modifygroup%s' %(self.url_suffix)
		self.URLMap[self.selectGroupReport] = 'selectgroup%s' %(self.url_suffix)
		self.URLMap[self.saveAsNewReport] = 'savegroup%s' %(self.url_suffix)
		self.URLMap[self.onNavigate] = 'navigate'
		self.URLMap[self.getStatsList] = 'statlist'
		self.URLMap[self.getResourceFilters] = 'getresourcefilters'
		self.URLMap[self.getReportByCategory] = 'get%s' %(self.url_suffix)
		self.severityMap = {0: "v_green.gif", 1: "v_yellow.gif", 2: "v_red.gif"}
		self.real_time_server = self.unms.real_time_server
		self.URLMap[self.addToRealTime] = 'addtorealtime%s' %(self.url_suffix)
		self.URLMap[self.getRealTimeReport] = 'getrealtime%s' %(self.url_suffix)
		self.purge_dict={}
		purge_objs=DatabaseServer.executeSQLStatement('select * from tblpurge')
		map(lambda x:self.purge_dict.update({x['tname']:x['limitval']}),purge_objs)

	def updateStatMapObjs(self):
		sql_stmt = queryStandardizer.getQuery('Q655')
		objs = DatabaseServer.executeSQLStatement(sql_stmt)
		self.stat_count = len(objs)
		if objs and objs != -1:
			map(lambda a, b = self.statname_id_map: b.update({a.get('name', ''): safe_int(a.get('statid'))}), objs)
			map(lambda a, b = self.statid_name_map: b.update({safe_int(a.get('statid')): a.get('name', '')}), objs)
			map(lambda a, b = self.rawdn_id_map: b.update({a.get('rawdn', ''): safe_int(a.get('statid'))}), objs)
			map(lambda a, b = self.statid_dn_map: b.update({safe_int(a.get('statid')): a.get('rawdn', '')}), objs)
			map(lambda a, b = self.statid_unit_map: b.update({safe_int(a.get('statid', '')): a.get('unit')}), objs)

	def publishObjs(self, request):
		"""Main function to construct the Report.
		Get the report & sub report informations from the database for the 
		selected reportdn. or construct the report informations based on the 
		statistics. And call the basePublishObjs to construct the report.
		"""
		try:
			user_info = request.get('__user', {})
			st_time = time.time()
			self.checkStatCount()
			# Construct the report_dict
			report_dict = {'request': request, 'ret': {}}
			report_dict.update(request)
			# For Statistics Report
			if report_dict.get('resid') and report_dict.get('resid') != 'None' :
				self.getStatsReportProfile(report_dict)
				report_list_objs = []
			#Normal Report profile.
			else:
				report_obj = self.getDefaultReport(report_dict)
				if (not report_dict.get('resolution',"")) :
					report_dict['resolution'] = report_obj.get('resolution',"")
					report_dict['request']['resolution'] = report_obj.get('resolution',"")
				report_id = safe_int(report_dict.get('reportdn'))
				report_list_objs = self.report_profile.getReportListObjs(filter_condn=["reportid = %d" %(report_id)])
				for report_list in report_list_objs:
					report_list['statids'] = map(lambda a, b = self.statname_id_map: safe_int(b.get(a.strip())), string.split(report_list.get('stats', ''), ' | '))
				report_dict['report_obj'] = report_obj
				report_dict['sub_reports'] = report_list_objs
				report_dict['business_hr'] = safe_int(report_obj.get('business_hr',0))
				report_dict['business_hr_profile'] = report_obj.get('business_hr_profile','')
			
			ret = self.basePublishObjs(report_dict)
			report_obj = report_dict.get('report_obj', {})
			request["report_obj"]= report_obj
			if isPortalAccount(user_info) and (request.has_key("isfromND") and safe_int(request.get("isfromND","")) == 1):
				customer_report_name = str(user_info.get('dn', '')) + " Traffic Report"
				ret['title'] = customer_report_name
				ret['title_heading'] = XMLOptionText(customer_report_name)
			else:	
				ret['title'] = report_obj.get('name', '')
				ret['title_heading'] = XMLOptionText(report_obj.get('name', ''))
			# Only for Report - Center Alignment.
			ret['align'] = makeAttr('align', 'center')
			if report_dict.has_key('display_str') and not report_dict.get('autoReport'):
				ret['content'].append(XMLJS("alert('%s')" %(report_dict['display_str'])))
			if report_dict.has_key('created_charts') and report_dict.get('created_charts'):
				request['created_charts'] = report_dict.get('created_charts', [])				
			min_max_array = {'Minimize': ChangeName('Minimize'), 'Maximize': ChangeName('Maximize'), 'Display_One': ChangeName('Display One'), 'Display_All': ChangeName('Display All')}
			resolution_array = {
								'Select': ChangeName('Select Resolution'), 'Minute': ChangeName('Minute'), 'Hour': ChangeName('Hour'), 'Day': ChangeName('Day'), 'Month': ChangeName('Month'),
								'Select_Pattern': ChangeName('Select Pattern'), 'Hour_Day': ChangeName('Hour of Day'), 'Day_Week': ChangeName('Day of Week'), 'Day_Month': ChangeName('Day of Month'), 'Month_Year': ChangeName('Month of Year'),
						}
			js_content = XMLJS('var MIN_MAX = %s;\n var RESOLUTION = %s;\n' %(convertPyDictToJSMap(min_max_array), convertPyDictToJSMap(resolution_array)))
			ret['content'] = [js_content, ret['content']]
			if ((request.get('report_type','') == 'data' or self.checkDataTable(report_list_objs)) and (safe_int(request.get('topn',1)) ==0 or safe_int(report_obj.get('topn',1)) ==0 ) and safe_int(report_dict.get('no_resource',1)) > 21 and (not report_dict.get('fromPDF'))):
				content = self.generatePDF(request,ret)
				content_replace = XMLContentTable([XMLTable(content, classId='alignedTableForm')])
				ret['content'] = self.getContentReplace(ret['content'],content_replace)
			long_reports = ['downtime_report','data','summary','events','device_downtime_summary_report','traps_summary','traps','nodes_summary','vsatpingreport','nw_efficiency','nw_statistic','uptime_summary','backhaul','backhaul_monthly','statewise_summary']
			if (report_dict.get('report_type','') in long_reports and safe_int(report_dict.get('no_resource',1)) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY) and (not report_dict.get('fromPDF')):
				content = self.generatePDF(request,ret)
				content_replace = XMLContentTable([XMLTable(content, classId='alignedTableForm')], table_heading=ret.get('title', ''))
				ret['content'] = self.getContentReplace(ret['content'],content_replace)
			et_time = time.time()
			logMsg(2, 'Time taken to generate report : %s' %(et_time - st_time), self.module_name)
			return ret
		except CallNetflowReportException, msg:
			return {"content" : XMLRedirect('nfReport?reportdn=%s'%msg)}
		except NoReportException:
			msg = ChangeName('No Report is configured. Please configure some report')
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report')}
		except NoDefaultReportProfileConfigured:
			return {'content': XMLRedirect('Report')}
		except Exception, msg:
			msg = 'Exception while generating the report - %s' %(msg)
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report')}
		except:
			msg = 'Exception while generating the report'
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report')}

	def getContentReplace(self,data,content) :
		try :
			data_type = type(data)
			if data_type == types.ListType or data_type==types.TupleType :
				result = []
				for dat in data :
					data_val = self.getContentReplace(dat,content)
					result.append(data_val)
				return result	
			elif (isinstance(data,XMLMainReportTable)) :
				data_val = content
			else :
				data_val = data
		except Exception, msg:
			msg = 'Exception while generating the report - %s' %(msg)
			logExceptionMsg(4, msg, self.module_name)
			data_val = data
		return data_val	
		
	def generatePDF(self,request,ret) :
		request['ret_content'] = ret
		request['pdf_url'] = '/report?'
		request['pdf_path'] = 'report'
		request['format_csv'] = 1
		#request['format_csv'] = 2
		request['autoreport'] = 1
		request['isFromReport'] = 1

		link = self.unms.pdf_creator.createPDF(request)
		logMsg(2, 'Generated XLS File - %s'%link, self.module_name)
		# the validation is added as the link is send as a dictionary of XMLContentTable instance when there is an error with the PDF
		# So Displaying the error message in place of the csv format file link
		if type(link) == types.StringType:
			request['report_file_path'] = string.replace(link, os.sep, '\\\\')
			content = [[XMLText('The result of this report is beyond display capacity in HTML format. Please click the link below to view the results in CSV format.',classId="stat_contentstext")],[XMLLink(link, ret['title'] + ".csv",target='_blank')]]
			#content = [[XMLText('The result of this report is beyond display capacity in HTML format. Please click the link below to view the results in Excel format.',classId="stat_contentstext")],[XMLLink(link, ret['title'] + ".xls",target='_blank')]]
		else:
			content = [[XMLText('Error in generating the Report. Please click the other Report format')]]
		return content
		
	def checkDataTable(self, report_list_objs) :
		try :	
			ret_val = 0
			for report_list in report_list_objs :
				if report_list.get('reporttype') == 'data' :
					ret_val = 1
					break
		except Exception, msg:
			msg = 'Exception in checkDataTable - %s' %(msg)
			logExceptionMsg(4, msg, self.module_name)
			ret_val = 0
		return ret_val	

	def updateStatInMemory(self, statid):
		""" Update the stat in memory for the dynamic stats.
		Port monitoring stats, Cisco SAA stats
		"""
		#sql_stmt = 'SELECT * FROM tblStatMap WHERE statid = %d' %(statid)
		sql_stmt = queryStandardizer.getQuery('Q014') %(safe_int(statid))
		objs = DatabaseServer.executeSQLStatement(sql_stmt)
		if objs != [] and objs != -1:
			obj = objs[0]
			self.statname_id_map[obj.get('name', '')] = safe_int(obj.get('statid'))
			self.statid_name_map[safe_int(obj.get('name', ''))] = obj.get('name', '')
			self.rawdn_id_map[obj.get('rawdn', '')] = safe_int(obj.get('statid'))
			self.statid_dn_map[safe_int(obj.get('statid'))] = obj.get('rawdn', '')
			self.statid_unit_map[safe_int(obj.get('statid'))] = obj.get('unit', '')

	def getStatsReportProfile(self, report_dict):
		"""Get the report obj, and report list obj for the 
		statistics level report.
		"""		
		res_ids = report_dict.get('resid')
		if res_ids:
			resids = map(lambda a: safe_int(a), string.split(res_ids, ','))
		else:
			resids = []
		view_type = safe_int(report_dict.get('View_type'))
		if not view_type:
			if len(resids) == 1:
				obj = self.resource_manager.getObj(resids[0])
				resObjs = [obj]
			else:
				# to solve bug id : 9882
				user_info = report_dict.get("request", "").get('__user', {})
				if isPortalAccount(user_info) and ( report_dict.get("request", "").has_key("isfromND") and safe_int( report_dict.get("request", "").get("isfromND","")) == 1):
					resObjs = self.resource_manager.getObjs(filters=['resid IN (%s)' %(res_ids), path_filter], flag=1)
				else:
					resObjs = self.resource_manager.getObjs(filters=['resid IN (%s)' %(res_ids)], flag=1)
		else:
			table_map = {1: 'Location', 2: 'Domain'}
			if table_map.get(view_type, 'Location') == "Location":
				sql_stmt = 'SELECT pathid,name,parentid,locid,PBS as "PBS",resid,res_count,res_poll_state,alarm_count,severity from tblLocationPathInfo WHERE resid IN (%s)'%res_ids
			else:
				sql_stmt = 'SELECT pathid,name,parentid,domid,PBS as "PBS",resid,res_count,res_poll_state,alarm_count,severity from tblDomainPathInfo WHERE resid IN (%s)'%res_ids
			resObjs = DatabaseServer.executeSQLStatement(sql_stmt)
		stat_ids = report_dict.get('statids', '')
		if stat_ids:
			statids = map(lambda a: safe_int(a), string.split(stat_ids, ','))
		else:
			statids = []
		# get the common path from the available path list
		path_list = map(lambda a: a.get('PBS', ''), resObjs)
		common_path = getCommonPath(path_list)
		path_name = makeRelativePath(common_path, report_dict.get('root_path', 'root'))
		if path_name == None:
			path_name = ''
		report_obj = {
				'reportid': -1, 'name': path_name, 'category': 'Statistical Reports', 'pathFmt': 0, 'viewtype': view_type,
				'pathnames': path_name, 'user_list': 'Public', 'timesel': 1, 'resolution': 'raw', 
				'resfilter': ' r.resid in (%s) ' %(res_ids), 
			}
		report_list_objs = []
		if len(resids) <= 1:
			for statid in statids:
				# if suppose, statid is not in memory, then add it dynamically.
				if not self.statid_name_map.has_key(safe_int(statid)):
					self.updateStatInMemory(statid)
				group_name = self.statid_name_map.get(statid, '') + ' Report'
				# For stats level report, make sure that all the sub reports
				# have the negative values to avoid the confusion with the actual sub reports
				# which are present in the tblReportList.
				report_list_id =  -1 * safe_int(statid)
				width = safe_int(report_dict.get('width', DEFAULT_REPORT_WIDTH))
				if not width:
					width = DEFAULT_REPORT_WIDTH
				obj = {
						'reportlistid': report_list_id, 'reportid': -1, 'name': group_name, 'statids': [statid], 
						'reporttype': 'trend', 'group_type': 0, 'width': width, 'height': 270, 'font': 's','display' : 'avg',
					}
				report_list_objs.append(obj)
		else:
			# if suppose, statid is not in memory, then add it dynamically.
			for statid in statids:
				if not self.statid_name_map.has_key(safe_int(statid)):
					self.updateStatInMemory(statid)
				group_name = self.statid_name_map.get(statid, '') + ' Report'
			# For stats level report, make sure that all the sub reports
			# have the negative values to avoid the confusion with the actual sub reports
			# which are present in the tblReportList.
			report_list_id =  -1 
			user_info = report_dict.get("request", "").get('__user', {})
			if isPortalAccount(user_info) and ( report_dict.get("request", "").has_key("isfromND") and safe_int( report_dict.get("request", "").get("isfromND","")) == 1):
				obj = {
						'reportlistid': report_list_id, 'reportid': -1, 'name': group_name, 'statids': statids, 'topn': 0,
						'reporttype': 'trend', 'group_type': 0, 'width': DEFAULT_REPORT_WIDTH, 'height': 270, 'font': 's','display' : 'avg',
					}
			else:
				obj = {
						'reportlistid': report_list_id, 'reportid': -1, 'name': group_name, 'statids': statids, 'topn': 0,
						'reporttype': 'trend', 'group_type': 1, 'width': DEFAULT_REPORT_WIDTH, 'height': 270, 'font': 's','display' : 'avg',
					}			
			report_list_objs.append(obj)
			report_dict['topn'] = 0		
		report_dict['report_obj'] = report_obj
		report_dict['sub_reports'] = report_list_objs
		report_dict['reportdn'] = -1		

	def getDefaultReport(self, request):
		"""Get the default report
		"""
		default_report = safe_int(request.get('__user', {}).get('default_report'))
		if request.get('reportdn'):
			report_id = safe_int(request.get('reportdn'))
			report_obj = self.report_profile.getReportObj(report_id)
		else:
			if default_report:
				report_obj = self.report_profile.getReportObj(default_report)
			else:
				# if default report is not selected, then redirect to report profile
				# configuration page
				raise NoDefaultReportProfileConfigured
		report_id = safe_int(report_obj.get('reportid'))
		if safe_int(report_obj.get('reporttype')) == 2 :
			raise CallNetflowReportException,str(report_id)		
		request['reportdn'] = report_id
		# current one is default report
		if report_id == default_report:
			report_obj['isDefault'] = 1
		else:
			report_obj['isDefault'] = 0
		return report_obj

	def basePublishObjs(self, report_dict):
		"""Get all the informations required to display in the report page
		like left panel, main report content...
		"""
		try:
			self.getPathFilters(report_dict)
			self.getResFilters(report_dict)
			self.getResourcesWithinFolder(report_dict)
			self.getTimeFilters(report_dict)
			report_dict['time_format'] = getReportParams(report_dict).time_format_map.get(report_dict.get('resolution', 'raw'))
			report_dict['request']['init_option'] = report_dict['init_option']
			#report_dict['request']['timescale'] = report_dict['timescale']
			#Modified by Sathish
			if report_dict.has_key('timescale'):
				report_dict['request']['timescale'] = report_dict['timescale']
			#end here
			if report_dict['st'] >= report_dict['et']:
				msg = [XMLText('End Time should be greater than Start Time. Please select the time'), BR, BR]
				content = XMLContentTable(msg, table_heading='Invalid Time Range')
				content = content + [
							XMLHiddenField('reportdn', safe_int(report_dict.get('reportdn'))),
							XMLHiddenField('resid', report_dict.get('resid')),
							XMLHiddenField('statids', report_dict.get('statids')),
							XMLHiddenField('pattern_type', safe_int(report_dict.get('pattern_type'))),
							#XMLHiddenField('report_group_type', report_dict.get('report_group_type', '')),
						]
				report_dict['ret']['content'] = content
			else:
				self.getMyPublishObjs(report_dict)
			if safe_int(report_dict.get('fromMobile')):
				return report_dict.get('ret', {})
			if not safe_int(report_dict.get('modify')) and not safe_int(report_dict.get('popup_report')):
				self.getLeftPanel(report_dict)
			self.getImageLinks(report_dict)
			return report_dict.get('ret', {})
		except Exception,msg:
			logExceptionMsg(4, "exception in basePublishObjs -- %s"%(msg), self.plural_name)

	def getPathFilters(self, report_dict):
		"""Form the path filter from the user selected path filter.
		"""
		view_type = safe_int(report_dict.get('report_obj', {}).get('viewtype'))
		# Change for CMC to get path for domain view only
		if safe_int(report_dict.get('request', {}).get('__user',{}).get('multitenancy')) == 1 and report_dict.get('request', {}).get('__user',{}).get('selectedRights') in ['ReadOnly','Operator'] and safe_int(report_dict.get('request', {}).get('__user').get('default_vtype'))==2:
			view_type = 2
		path_filter = "(p.PBS ILIKE '%s/%%' OR p.PBS = '%s')"
		actual_path_dict = {}
		# For Resource View
		if view_type == 0:
			root_path_key = 'root_path'
		# For Location View
		elif view_type == 1:
			root_path_key = 'loc_path'
		# For Domain View
		elif view_type == 2:
			root_path_key = 'dom_path'
		elif view_type == 3:
			root_path_key = 'biz_path'
		report_dict['root_path_key'] = root_path_key
		user_root_path = report_dict.get('__user', {}).get(root_path_key, ROOT_PATH)
		path_names = report_dict.get('report_obj', {}).get('pathnames', '')
		if path_names == None:
			path_names = ''
		path_list = string.split(path_names, ' | ')
		actual_path_filters = []
		# If only 'root' is selected.,
		if len(path_list) == 0:
			actual_path_filters.append(path_filter %(user_root_path, user_root_path))
		else:
			# some paths are selected.
			for path in path_list:
				path = string.strip(path)
				# if suppose, 'root' is selected, then replace root with the root path.
				if path == '' or path == ROOT_PATH:
					new_path = user_root_path
				else:
					# for report profile one
					if safe_int(report_dict.get('reportdn')) and safe_int(report_dict.get('reportdn')) != -1:
						if not(path.startswith(ROOT_PATH + PATH_SEP)):
							path = ROOT_PATH + PATH_SEP + path
					# for AdHoc report
					else:
						#added for the Vmax USer report fix -- bug 556 -- Everest 3.0
						if not(path.startswith(ROOT_PATH + PATH_SEP)):
							path = ROOT_PATH + PATH_SEP + path
						if not(path.startswith(user_root_path + PATH_SEP)):
							path = user_root_path + PATH_SEP + path
						#added for the Vmax USer report fix -- bug 556 -- Everest 3.0
					# if report configured for the same path as user
					if user_root_path == path:
						new_path = user_root_path
					# if report configured for a higher path than user
					elif user_root_path.find(path) != -1:
						new_path = user_root_path
					# if report configured for a lower path than user
					elif path.find(user_root_path) != -1:
						#added for the Vmax USer report fix -- bug 556 -- Everest 3.0
						if not(path.startswith(ROOT_PATH + PATH_SEP)):
							path = ROOT_PATH + PATH_SEP + path
						#added for the Vmax USer report fix -- bug 556 -- Everest 3.0
						new_path = path
					# both are not related to each other
					else:
						new_path = ''
						report_dict['not_my_accessible_path'] = 1
				# add root as prefix for the path, if root is not there.
				if new_path:
					actual_path_filters.append(path_filter %(new_path, new_path))
					actual_path_dict.update({new_path : path_filter %(new_path, new_path)})
		actual_path_filters = unique(actual_path_filters)
		path_filters = ''
		if actual_path_filters:
			path_filters = '(%s)' %(string.join(actual_path_filters, ' OR '))
		logMsg(2, 'Path Filter - %s' %(`path_filters`), self.module_name)
		report_dict['path_filter'] = path_filters
		report_dict['actual_path_filters'] = actual_path_filters
		report_dict['actual_path_dict'] = actual_path_dict

	def getResFilters(self, report_dict):
		"""Get the resource filters for the report.
		"""
		res_filter = report_dict.get('report_obj', {}).get('resfilter', '')
		if res_filter == None:
			res_filter = ''
		filters = [('resid', 'r.resid'), ('res_name', 'r.name'), ('respbs', 'p.PBS'), ('poll_addr', 'r.poll_addr'), ('node_type', 'n.node_type'), ('device_name', 'n.device_name'), ('alias', 'v.alias'), ('descr', 'v.descr'), ('restype', 'v.restype'), ('node_alias', 'n.alias'), ('node_descr', 'n.descr'), ('params', 'r.params')]
		for key, db_key in filters:
			if res_filter.find(' %s ' %(key)) != -1:
				res_filter = string.replace(res_filter, ' %s ' %(key), ' %s ' %(db_key))
		logMsg(2, 'Resource Filter - %s' %(`res_filter`), self.module_name)
		report_dict['res_filter'] = res_filter

	def getResourcesWithinFolder(self, report_dict, folder_summary=0, single_folder=''):
		"""Get all resources that are available in the configured folders as 
		well as satisfy the specifed conditions.
		"""
		DatabaseServer = getDBInstance()
		res_hostname_map = {}
		res_portal_displayname_map = {}
		res_hostip_map = {}
		res_path_map = {}
		res_actual_path_map = {}
		root_path = report_dict.get(report_dict.get('root_path_key'), ROOT_PATH)
		# Changed for Getting the Resource under single path
		if not folder_summary :
			path_filter = report_dict.get('path_filter', '')
		else :
			path_filter = single_folder
		path_filter = report_dict.get('path_filter', '')
		res_filter = report_dict.get('res_filter', '')
		filter_condn = ""
		if path_filter:
			filter_condn += " AND " + path_filter
		if res_filter:
			res_filter = string.replace(res_filter, '"', "'")
			filter_condn += " AND " + "(%s)" %(res_filter)
		if not path_filter and not res_filter:
			filter_condn = ""
		try:
			if report_dict.get('col_path_filter','').strip():
				filter_condn +=  " AND " + report_dict.get('col_path_filter')
		except:
			pass
		try:
			if report_dict.get('col_ip_filter','').strip():
				filter_condn += " AND " + "(%s)" %(report_dict.get('col_ip_filter'))
				if res_filter:
					res_filter = res_filter + " AND " + "(%s)" %(report_dict.get('col_ip_filter'))
				else:
					res_filter = report_dict.get('col_ip_filter')
		except:
			pass
		try:
			if report_dict.get('col_node_filter','').strip():
				filter_condn += " AND " + "(%s)" %(report_dict.get('col_node_filter'))
				if res_filter:
					res_filter = res_filter + " AND " + "(%s)" %(report_dict.get('col_node_filter'))
				else:
					res_filter = report_dict.get('col_node_filter')
		except:
			pass

		st_time = time.time()
		logMsg(2, 'Filter condition for getting resource information - %s' %(`filter_condn`), self.module_name)
		path_format = safe_int(report_dict.get('report_obj', {}).get('pathFmt'))
		view_type = safe_int(report_dict.get('report_obj', {}).get('viewtype'))
		# CMC Multi Tenancy : For handling the path 
		domain_path_filter=''
		filterCondn_path=''
		if safe_int(report_dict.get('request', {}).get('__user',{}).get('multitenancy')) == 1 and report_dict.get('request', {}).get('__user',{}).get('selectedRights') in ['ReadOnly','Operator'] and safe_int(report_dict.get('request', {}).get('__user').get('default_vtype'))==2:
			domain_path_filter = report_dict.get('request', {}).get('__user',{}).get('dom_path')
			filterCondn_path = "AND r.resid in (select resid from  tblDomainPathInfo where PBS = '%s' or PBS ILIKE '%s/%%')"%(domain_path_filter,domain_path_filter)
		# For Resource View
		if view_type == 0:
			#sql_stmt = "SELECT r.poll_addr, n.hostname, n.alias node_alias, v.alias res_alias, r.name, p.PBS, r.resid, p.pathid FROM tblResConfig r, tblResView v, tblPathInfo p, tblNodeInfo n WHERE r.resid = v.resid AND r.pathid = p.pathid AND r.poll_addr = n.poll_addr %s" %(filter_condn)
			sql_stmt = queryStandardizer.getQuery('Q656') %(filter_condn)
			if res_filter:
				res_query = 'SELECT r.resid, r.name, r.profile, p.PBS as "PBS", r.creationTime  FROM tblResConfig r, tblResView v, tblPathInfo p, tblNodeInfo n WHERE r.resid = v.resid AND r.pathid = p.pathid AND r.isDeleted = 0 AND r.poll_addr = n.poll_addr AND r.poller_id = n.poller_id %s' %(filter_condn)
			else:
				res_query = 'SELECT r.resid, r.name, r.profile, p.PBS as "PBS", r.creationTime  FROM tblResConfig r, tblPathInfo p WHERE r.pathid = p.pathid AND r.isDeleted = 0 %s' %(filter_condn)
		# For Location & Domain View
		else:
			view_name = iif(view_type == 1, 'Location', iif(view_type == 2,'Domain','BusinessView'))
			#sql_stmt = "SELECT pr.processName AS poller,r.poll_addr, case when n.ex_type != '' THEN n.ex_type ELSE n.hostname END hostname, case when n.ex_type != '' THEN n.ex_type ELSE n.alias END node_alias, v.alias res_alias, r.name, p.PBS as \"PBS\", r.resid, p.pathid FROM tblProcess pr,tblResConfig r, tblResView v, tbl%sPathInfo p, tblNodeInfo n WHERE r.resid = v.resid AND r.resid = p.resid AND r.poll_addr = n.poll_addr AND r.poller_id = n.poller_id  AND r.poller_id = pr.processId %s  %s " %(view_name, filter_condn, filterCondn_path)
			sql_stmt = "SELECT pr.processName AS poller,r.poll_addr, n.ex_type portal_display_name, n.hostname, n.alias node_alias, v.alias res_alias, r.name, p.PBS as \"PBS\", r.resid, p.pathid FROM tblProcess pr,tblResConfig r, tblResView v, tbl%sPathInfo p, tblNodeInfo n WHERE r.resid = v.resid AND r.resid = p.resid AND r.poll_addr = n.poll_addr AND r.poller_id = n.poller_id  AND r.poller_id = pr.processId %s  %s " %(view_name, filter_condn, filterCondn_path)
			#sql_stmt = queryStandardizer.getQuery('Q657') %(view_name, filter_condn) 
			if res_filter:
				res_query = 'SELECT r.resid, r.name, r.profile, p.PBS as "PBS", r.creationTime FROM tblResConfig r, tblResView v, tbl%sPathInfo p, tblNodeInfo n WHERE r.resid = v.resid AND r.resid = p.resid AND r.isDeleted = 0 AND r.poll_addr = n.poll_addr AND r.poller_id = n.poller_id %s' %(view_name, filter_condn)
			else:
				res_query = 'SELECT r.resid, r.name, r.profile, p.PBS as "PBS", r.creationTime FROM tblResConfig r, tbl%sPathInfo p WHERE r.resid = p.resid AND r.isDeleted = 0 %s' %(view_name, filter_condn)
		logMsg(2, 'Sql Statement to get the path - %s' %(`sql_stmt`), self.module_name)
		path_objs = DatabaseServer.executeSQLStatement(sql_stmt, module=self.dblog_module)
		if path_objs and path_objs != -1:
			# Get alias path name
			if not path_format:
				self.getAliasPathName(path_objs, report_dict['request'])
			# Update the Resource Path Map.
			for obj in path_objs:
				# ensure that key with upper case is available.
				if obj.has_key('pbs'):
					obj['PBS'] = obj.get('pbs', '')
				# For Path Name
				if path_format:
					path = makeRelativePath(obj.get('PBS', ''), root_path)
					if not path.startswith(ROOT_PATH + PATH_SEP):
						path = ROOT_PATH + PATH_SEP + path
				# For Alias
				else:
					path = obj.get('respbs', '')
					# add the site name if it is msp setup and foresight configuration
					if maxInputStats.MSP_SETUP == 1 and maxInputStats.FORESITE == 1:
						path = obj.get('poller') + PATH_SEP + path
				res_hostname_map[safe_int(obj.get('resid'))] = obj.get('hostname','')
				res_portal_displayname_map[safe_int(obj.get('resid'))] = obj.get('portal_display_name','')
				res_hostip_map[safe_int(obj.get('resid'))] = obj.get('poll_addr','')
				res_path_map[safe_int(obj.get('resid'))] = path
				res_actual_path_map[safe_int(obj.get('resid'))] = getURLString(makeRelativePath(obj.get('PBS', ''), root_path))
		report_dict['res_query'] = res_query
		report_dict['res_actual_path_map'] = res_actual_path_map
		report_dict['res_hostname_map'] = res_hostname_map
		report_dict['res_portal_displayname_map'] = res_portal_displayname_map
		report_dict['res_hostip_map'] = res_hostip_map
		et_time = time.time()
		if not folder_summary :
			report_dict['res_path_map'] = res_path_map
		else :	
			# Called from Folder Summary Report
			return path_objs
		logMsg(2, 'Time taken to get the path - %s' %(et_time - st_time), self.module_name)

	def getAliasPathName(self, objs, request):
		"""Get the alias path name for each selected resource.
		First take the node alias. if node alias is empty, then take the resource alias.
		if resource alias also empty, then take the hostname. 
		if hostname also empty, then poll_addr.
		"""
		for obj in objs:
			if obj.get('node_alias', ''):
				obj['respbs'] = obj['node_alias'] + PATH_SEP + obj.get('name', '')
			elif obj.get('res_alias', '') and obj.get('res_alias', '') not in ['custom_poll', '*=custom_poll']:
				res_alias = self.getAliasName(obj.get('res_alias', ''), request.get('__user', {}).get('dn', ''))
				if res_alias:
					obj['respbs'] = res_alias
				else:
					if obj.get('hostname', '')  :
						obj['respbs'] = obj.get('hostname', '') + PATH_SEP + obj.get('name', '')
					else:
						obj['respbs'] = obj.get('poll_addr', '') + PATH_SEP + obj.get('name', '')
			elif obj.get('node_alias', ''):
				obj['respbs'] = obj['node_alias'] + PATH_SEP + obj.get('name', '')
			elif obj.get('hostname', '') :
				obj['respbs'] = obj.get('hostname', '') + PATH_SEP + obj.get('name', '')
			else:
				obj['respbs'] = obj.get('poll_addr', '') + PATH_SEP + obj.get('name', '')

	def getAliasName(self, alias, user):
		"""Get the user based alias name for the resource.
		"""
		default_alias = ''
		if alias and alias != 'None' and alias != '*=': 
			if user == 'administrator':
				user = '*'
			user = user + '='
			aliases = alias.split(',')
			for alias_name in aliases :
				if alias_name.startswith('*='):
					uname, aname = alias_name.split("=")
					default_alias = aname
				if alias_name.startswith(user) :
					uname, aname = alias_name.split("=")
					return aname
		return default_alias

	def getMyPublishObjs(self, report_dict):
		"""Get the datas for each sub reports and draw the graph
		"""
		try :
			st_time = time.time()
			res_path_map = report_dict.get('res_path_map', {})
			non_list = [None, 'None', "", -1, "-1", "Null"]
			# check whether the path is accessible
			content=[]
			report_dict['C3_JS'] = []
			if report_dict.has_key('not_my_accessible_path'):
				msg = [XMLText('You are not authorized to see this Report'), BR, BR, BR, XMLBackButton('Back')]
				content.append(XMLContentTable(msg, table_heading='Access Denied'))
				content = content + [
							XMLHiddenField('reportdn', safe_int(report_dict.get('reportdn'))),
							XMLHiddenField('statids', report_dict.get('statids')),
							XMLHiddenField('pattern_type', safe_int(report_dict.get('pattern_type'))),
							XMLHiddenField('View_type', safe_int(report_dict.get('report_obj', {}).get('viewtype'))),
						]
				if report_dict.has_key('resid') and report_dict.get('resid'):
					content.append(XMLHiddenField('resid', report_dict.get('resid')))
			else:
				# check for any resources available for that report
				if not res_path_map:
					content = []
					if report_dict.has_key('not_my_accessible_path'):
						msg = [XMLText('You are not authorized to see this Report'), BR, BR, BR, XMLBackButton('Back')]
						content.append(XMLContentTable(msg, table_heading='Access Denied'))
					else:
						msg = [BR, ChangeName('No Resource Available to see this Report'), BR, BR, BR]
						content.append(XMLContentTable(msg, table_heading='Data Not Available'))
					content = content + [
								XMLHiddenField('reportdn', safe_int(report_dict.get('reportdn'))),
								XMLHiddenField('resid', report_dict.get('resid')),
								XMLHiddenField('statids', report_dict.get('statids')),
								XMLHiddenField('pattern_type', safe_int(report_dict.get('pattern_type'))),
								XMLHiddenField('View_type', safe_int(report_dict.get('report_obj', {}).get('viewtype'))),
							]
					if report_dict.has_key('resid') and report_dict.get('resid'):
						content.append(XMLHiddenField('resid', report_dict.get('resid')))
				else:
					content = []
					report_type = report_dict.get('report_type')
					pattern_type = safe_int(report_dict.get('pattern_type'))
					if report_type == 'pattern':
						resolution = getReportParams(report_dict).pattern_resolution_map.get(pattern_type)
						report_dict['resolution'] = resolution
					else:
						# reset the pattern type resolution to 0.
						report_dict['pattern_type'] = 0
					#if report_dict['report_obj'].has_key('timesel') and report_dict['report_obj']['timesel'] and report_dict['report_obj'].has_key('timebetween') and report_dict['report_obj']['timebetween']:
					#Added by sathish for output title time format display 
					if report_dict.has_key('start_time') and report_dict.get('start_time','')!='' and report_dict.get('start_time','')!=0 or report_dict['request'].has_key('timescale'):
						if report_dict['request'].get('timebetween','') or report_dict.get('timebetween',""): 
							start_duration = time.strftime('%d-%b-%Y', time.localtime(report_dict['st']))
							end_duration = time.strftime('%d-%b-%Y', time.localtime(report_dict['et']))
						elif report_dict['report_obj'].get('timebetween') not in non_list:
							start_duration = time.strftime('%d-%b-%Y', time.localtime(report_dict['st']))
							end_duration = time.strftime('%d-%b-%Y', time.localtime(report_dict['et']))							
						else:
							start_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(report_dict['st']))
							end_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(report_dict['et']))
					elif report_dict['report_obj'].has_key('t1_start_time') and report_dict['report_obj'].get('t1_start_time','')!=0 and  report_dict['report_obj'].get('t1_start_time','')!='':
						if report_dict['request'].get('timebetween','') or report_dict.get('timebetween',"") or report_dict['report_obj'].get('timebetween',''):
							start_duration = time.strftime('%d-%b-%Y', time.localtime(report_dict['report_obj']['t1_start_time']))
							end_duration = time.strftime('%d-%b-%Y', time.localtime(report_dict['report_obj']['t1_end_time']))
						elif report_dict['report_obj'].get('timebetween',''):
							start_duration = time.strftime('%d-%b-%Y', time.localtime(report_dict['report_obj']['t1_start_time']))
							end_duration = time.strftime('%d-%b-%Y', time.localtime(report_dict['report_obj']['t1_end_time']))
						else:
							start_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(report_dict['report_obj']['t1_start_time']))
							end_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(report_dict['report_obj']['t1_end_time']))
					else:

						start_duration = time.strftime('%d-%b-%Y', time.localtime(report_dict['st']))
						end_duration = time.strftime('%d-%b-%Y', time.localtime(report_dict['et']))
					#Added by sathish for output title time format with Time(timebetween) display 
					if report_dict['request'].has_key('timebetween') and report_dict.has_key('timebetween'):
						time_between = report_dict['request'].get('timebetween','')
						t_period = '%s - %s %s (%s)' %(start_duration, end_duration,time_between, getTimeZone())
					elif report_dict['report_obj'].has_key('timebetween'):
						time_between = report_dict['report_obj'].get('timebetween','')
						t_period = '%s - %s %s (%s)' %(start_duration, end_duration,time_between, getTimeZone())
					else:
						t_period = '%s - %s (%s)' %(start_duration, end_duration, getTimeZone())
					time_period = t_period
					#Added by sathish for update Time1 to start time for display in leftpanel custom time
					if report_dict.get('init_option','') == 3 and not report_dict['request'].has_key('start_time'):         # or report_dict['request'].has_key('start_time')!=						
						if report_dict['report_obj'].get('t1_start_time') !='' and report_dict['report_obj'].get('t1_start_time') != 0 and report_dict['report_obj'].get('t1_start_time') !='None':
							report_dict['request']['start_time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(report_dict['report_obj']['t1_start_time']))
							report_dict['request']['end_time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(report_dict['report_obj']['t1_end_time']))
					else:
						pass
					#end here						
					#time_period = '%s - %s' %(start_duration, end_duration)
					sub_reports = report_dict.get('sub_reports', [])
					report_data = []
					report_id = safe_int(report_dict.get('reportdn'))
					# For normal report profile one.
					main_report = 1
					# For Popup Report
					if safe_int(report_dict.get('popup_report')):
						main_report = 2
					# For Statistical Report
					if report_id == -1:
						main_report = 0
						if len(sub_reports) > 1:
							main_report = 1
					if safe_int(report_dict.get('realtime')):
						main_report = 3
					header = not safe_int(report_dict.get('modify'))
					group_info_map = 'var group_info = new Array();'
					report_type = report_dict.get('report_type', '')
					if maxInputStats.ENABLE_C3JS_GRAPH:
						if report_type in ['line','spline','area','bar','line_bar','current_history']:
							report_dict['c3_report_type'] = report_type
							if report_type in  ['line_bar','current_history']:
								report_dict['display'] = 'all'
							report_type = 'trend'
					# Ginni - For C3JS Chart Support
					report_params = []
					report_dict['c3_chart_count'] = 1
					for sub_report in sub_reports:
						if sub_report.get('reporttype') == 'nw_statistic':
							if report_dict['request'].get('timebetween','') or report_dict.get('timebetween',"") or report_dict['report_obj'].get('timebetween',''):
								t1_st = report_dict.get('report_obj', {}).get('t1_start_time', '')							
								t1_start_duration = time.strftime('%d-%b-%Y', time.localtime(t1_st))							
								t1_et = report_dict.get('report_obj', {}).get('t1_end_time', '')							
								t1_end_duration = time.strftime('%d-%b-%Y', time.localtime(t1_et))
								t2_st = report_dict.get('report_obj', {}).get('t2_start_time', '')							
								t2_start_duration = time.strftime('%d-%b-%Y', time.localtime(t2_st))
								t2_et = report_dict.get('report_obj', {}).get('t2_end_time', '')							
								t2_end_duration = time.strftime('%d-%b-%Y', time.localtime(t2_et))
							elif report_dict['report_obj'].get('timebetween',''):
								t1_st = report_dict.get('report_obj', {}).get('t1_start_time', '')							
								t1_start_duration = time.strftime('%d-%b-%Y', time.localtime(t1_st))							
								t1_et = report_dict.get('report_obj', {}).get('t1_end_time', '')							
								t1_end_duration = time.strftime('%d-%b-%Y', time.localtime(t1_et))
								t2_st = report_dict.get('report_obj', {}).get('t2_start_time', '')							
								t2_start_duration = time.strftime('%d-%b-%Y', time.localtime(t2_st))
								t2_et = report_dict.get('report_obj', {}).get('t2_end_time', '')							
								t2_end_duration = time.strftime('%d-%b-%Y', time.localtime(t2_et))
							else:
								t1_st = report_dict.get('report_obj', {}).get('t1_start_time', '')							
								t1_start_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(t1_st))							
								t1_et = report_dict.get('report_obj', {}).get('t1_end_time', '')							
								t1_end_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(t1_et))
								t2_st = report_dict.get('report_obj', {}).get('t2_start_time', '')							
								t2_start_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(t2_st))
								t2_et = report_dict.get('report_obj', {}).get('t2_end_time', '')							
								t2_end_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(t2_et))
							# For Excel Report
							if safe_int(report_dict.get('fromPDF')) == 1 and safe_int(report_dict.get('format_csv')) == 1:
								time_period = ''
							elif safe_int(report_dict.get('fromPDF')) == 1:
								time_period = '\nTime 1: %s - %s (%s)\nTime 2: %s - %s (%s)' %(t1_start_duration,t1_end_duration, getTimeZone(), t2_start_duration, t2_end_duration, getTimeZone())
							else:
								time_period = 'Time 1: %s - %s (%s) Time 2: %s - %s (%s)' %(t1_start_duration,t1_end_duration, getTimeZone(), t2_start_duration, t2_end_duration, getTimeZone())
						if sub_report.get('reporttype') in['line_bar','current_history']:
							sub_report['display'] = 'all'
						# if sub_report.get('reporttype').find('mrtg')!=-1:
						# 	sub_report['display'] = 'all'
						width = safe_int(sub_report.get('width', report_dict.get('width', DEFAULT_REPORT_WIDTH)))
						sub_report['width'] = width
						report_dict['report_type'] = report_type
						#report_dict['report_type'] = sub_report.get('reporttype','')
						report_dict['time_period_heading'] = time_period
						report_list_id = safe_int(sub_report.get('reportlistid'))
						group_name = ChangeName(sub_report.get('name', ''))
						typeOfReport=0
						try:
							a= report_dict.get('report_obj')
							typeOfReport=a['reporttype']
						except:
							pass
						group_info_map += 'group_info[%d] = new Array();\n' %(report_list_id)
						if(typeOfReport == 4):
							sub_report_dict = self.makeSubReportDict(sub_report, report_dict)
							stats = sub_report.get('stats', '')
							if stats.find('$') != -1:
								sub_type, classifyBy, reportas = string.split(stats, '$')
							if report_dict.has_key('reportas'):
								sub_report['reportas'] = report_dict.get('reportas', 'detail')
							else:
								sub_report['reportas'] = reportas
								sub_report['sub_type'] = sub_type
								sub_report['classifyBy'] = classifyBy
							group_reports = self.makeFortigateGraph(sub_report, sub_report_dict)
							group_info = report_dict['sub_report_%d' %(report_list_id)].get('group', {})
							group_info_map += 'group_info[%d][0] = "%s";\n' %(report_list_id, group_info.get('st'))
							group_info_map += 'group_info[%d][1] = "%s";\n' %(report_list_id, group_info.get('et'))
							group_info_map += 'group_info[%d][2] = "%s";\n' %(report_list_id, report_dict.get('reportas', sub_report.get('reportas', 'detail')))
							group_info_map += 'group_info[%d][3] = "%s";\n' %(report_list_id, group_info.get('topn'))
							group_info_map += 'group_info[%d][4] = "%s";\n' %(report_list_id, group_info.get('report_type'))
							group_info_map += 'group_info[%d][7] = "%s";\n' %(report_list_id, group_info.get('pattern_type'))
							group_info_map += 'group_info[%d][8] = "%s";\n' %(report_list_id, group_info.get('timebetween',''))
						else:	
							group_reports = self.makeGraph(sub_report, report_dict)
							if report_dict.get('report_type').find('mrtg')!=-1:
								if ((report_dict.get('cal_val','')=='') and (report_dict.get('start_time','')=='')):
									time_period=report_dict.get('mrtg_time_period_heading')
							if report_dict.has_key('params'):
								report_params.append(report_dict.get('params', {}))
							group_info = report_dict['sub_report_%d' %(report_list_id)].get('group', {})
							group_info_map += 'group_info[%d] = new Array();\n' %(report_list_id)
							group_info_map += 'group_info[%d][0] = "%s";\n' %(report_list_id, group_info.get('st'))
							group_info_map += 'group_info[%d][1] = "%s";\n' %(report_list_id, group_info.get('et'))
							group_info_map += 'group_info[%d][2] = "%s";\n' %(report_list_id, group_info.get('resolution'))
							group_info_map += 'group_info[%d][3] = "%s";\n' %(report_list_id, group_info.get('topn'))
							group_info_map += 'group_info[%d][4] = "%s";\n' %(report_list_id, group_info.get('report_type'))
							group_info_map += 'group_info[%d][8] = "%s";\n' %(report_list_id, group_info.get('timebetween',''))
							if report_list_id < 0:
								group_info['resids'] = report_dict.get('resid')
								group_info['statids'] = -1 * report_list_id
							group_info_map += 'group_info[%d][5] = "%s";\n' %(report_list_id, group_info.get('resids'))
							group_info_map += 'group_info[%d][6] = "%s";\n' %(report_list_id, group_info.get('statids'))
							group_info_map += 'group_info[%d][7] = "%s";\n' %(report_list_id, group_info.get('pattern_type'))
							group_info_map += 'group_info[%d][8] = "%s";\n' %(report_list_id, group_info.get('timebetween',''))
						time_period_heading = time_period
						if sub_report.get('reporttype') == 'nw_statistic' and time_period and safe_int(report_dict.get('no_resource',1)) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY:
							time_period = ''
						if report_dict.has_key('condn_str') and report_dict.get('condn_str'):
							time_period_heading += ' %s' %report_dict.get('condn_str', '')
						popup_link = ''
						isModifyButton = 1
						if report_list_id > 0:
							popup_link = 'javascript:openGroupReport(%d);' %(report_list_id)
							isModifyButton = 0
						if(typeOfReport == 4):
							popup_link = ''
						user_info = report_dict.get('__user', {})
						if isPortalAccount(user_info) or isReadOnlyAccount(user_info):
							popup_link = ''
						#if isWebPortalEverest():	#Disable all the minimize and max buttons in portal
						#	popup_link = ''
						if report_dict.get('image_only') or safe_int(report_dict.get('fromMobile')):
							report_data.extend(group_reports)
							#bug fix 3589
							if report_dict.get('C3_JS') and not safe_int(report_dict.get('fromMobile')):
								report_data.extend(report_dict.get('C3_JS'))
						else:
							#report_data.append(XMLReportTable(report_list_id, group_reports, report_width=width, table_heading=group_name, time_period=time_period, header_class=report_dict.get('header_class', 'Reports'), popup_link=popup_link, header=header, main_report=main_report))
							# for the report generation when redirected form the Network Diagram
							user_info = report_dict.get("request", "").get('__user', {})
							if isPortalAccount(user_info) and ( report_dict.get("request", "").has_key("isfromND") and safe_int( report_dict.get("request", "").get("isfromND","")) == 1):
								stats_list = []
								stats_list = report_dict.get('statids').split(",")
								i=0;
								for reports in group_reports:
									try:
										group_name = str(report_dict['sub_report_%d' %(report_list_id)]['title_map'][safe_int(stats_list[i])]['y_title']) + " Report"
									except Exception, msg:
										logExceptionMsg(4, 'Exception in getMyPublishObjs - %s' %(msg), self.module_name)
									i+=1
									report_table_id = i
									report_data.append(XMLReportTable(report_table_id, [reports], report_width=width, table_heading=group_name, time_period=time_period, header_class=report_dict.get('header_class', 'Reports'), popup_link=popup_link, header=header, main_report=main_report))
							else:
								report_data.append(XMLReportTable(report_list_id, group_reports, report_width=width, table_heading=group_name, time_period=time_period, header_class=report_dict.get('header_class', 'Reports'), popup_link=popup_link, header=header, main_report=main_report))
						report_dict['c3_chart_count'] = report_dict['c3_chart_count'] + 1
					if report_dict.get('jsgraphs'):
						if report_dict.get('group_type') == 1:
							content = group_reports
						else:
							content = report_params
					elif report_dict.get('modify'):
						content = report_data
						group_params = ['st', 'et', 'resolution', 'topn', 'report_type', 'resids', 'statids', 'pattern_type' ,'timebetween']
						group_infos = string.join(map(lambda a, b = group_info: str(b.get(a)), group_params), '$$')
						js_content = XMLJS('var sub_group_info = "%s"; parent.group_info[%s] = sub_group_info.split("$$"); parent.label_%s.innerText = "%s";' %(group_infos, report_list_id, abs(report_list_id), time_period))
						content = [report_data, js_content]
					elif safe_int(report_dict.get('fromMobile')):
						content = report_data
					elif report_dict.get('image_only'):
						content = XMLReportImage(report_data)
					else:
						# HTML Offline Report.
						if safe_int(report_dict.get('html_offline_report')):
							content = [XMLMainReportTable(report_data)]
						else:
							user_info = report_dict.get("request", "").get('__user', {})
							if isPortalAccount(user_info) and ( report_dict.get("request", "").has_key("isfromND") and safe_int( report_dict.get("request", "").get("isfromND","")) == 1):
								content = [
												XMLJS(group_info_map), 
												XMLMainReportTableNew(report_data), 
												XMLHiddenField('reportdn', safe_int(report_dict.get('reportdn'))),
												XMLHiddenField('resid', report_dict.get('resid')),
												XMLHiddenField('statids', report_dict.get('statids')),
												XMLHiddenField('pattern_type', safe_int(report_dict.get('pattern_type'))),
												XMLHiddenField('View_type', safe_int(report_dict.get('report_obj', {}).get('viewtype'))),
												XMLHiddenField('isfromND', 1),
												XMLHiddenField('pathid', report_dict.get('request', {}).get('pathid', '')),
												XMLJS('onChangeReportType("%s");' %(report_dict.get('report_type', ''))),
											]
							else:
								content = [
												XMLJS(group_info_map), 
												XMLMainReportTable(report_data), 
												XMLHiddenField('reportdn', safe_int(report_dict.get('reportdn'))),
												XMLHiddenField('resid', report_dict.get('resid')),
												XMLHiddenField('statids', report_dict.get('statids')),
												XMLHiddenField('pattern_type', safe_int(report_dict.get('pattern_type'))),
												XMLHiddenField('View_type', safe_int(report_dict.get('report_obj', {}).get('viewtype', report_dict.get("request", {}).get("View_type")))),
												XMLJS('onChangeReportType("%s");' %(report_dict.get('report_type', ''))),
											]
						if report_dict.has_key('resid') and report_dict.get('resid'):
							content.append(XMLHiddenField('resid', report_dict.get('resid')))
			report_dict['ret']['content'] = content
			if report_dict.get('C3_JS'):
				logMsg(2, "C3 Content %s"%report_dict['ret']['content'], self.module_name)
				report_dict['ret']['content'].extend(report_dict.get('C3_JS'))
			et_time = time.time()
			logMsg(2, 'Time taken to get My Publish Objs - %s' %(`et_time - st_time`), self.module_name)
		except Exception,msg:
			logExceptionMsg(4, "exception in getMyPublishObjs %s"%(msg), self.plural_name)

	def makeGraph(self, sub_report, report_dict):
		""" Make the graph for each group by calling the corresponding group function..
		"""
		sub_report_dict = self.makeSubReportDict(sub_report, report_dict)
		graph = self.makeSubGraphs(sub_report_dict)
		if sub_report_dict.has_key('mrtg_time_period_heading') :
			report_dict['mrtg_time_period_heading'] = sub_report_dict['mrtg_time_period_heading']
		if sub_report_dict.has_key('no_resource') :
			report_dict['no_resource'] = sub_report_dict['no_resource']
		if sub_report_dict.has_key('display_str'):
			report_dict['display_str'] = sub_report_dict['display_str']
		if sub_report_dict.has_key('created_charts'):
			report_dict['created_charts'] = report_dict.get('created_charts', []) + sub_report_dict.get('created_charts', [])
		if sub_report_dict.has_key('params'):
			report_dict['params'] = sub_report_dict.get('params', {})
		if maxInputStats.ENABLE_C3JS_GRAPH:
			if report_dict.has_key('c3_report_type') and report_dict.get('c3_report_type') != -1:
				report_dict['report_type'] = report_dict.get('c3_report_type')
		return graph

	def makeSubReportDict(self, sub_report, report_dict):
		"""Make the sub report dict.
		"""
		group_type = safe_int(sub_report.get('group_type'))
		report_type = report_dict.get('report_type', '')
		if not report_type:
			report_type = sub_report.get('reporttype', 'trend')
			# default report type is trend
			if not report_type:
				report_type = 'trend'
		# re-assign the report type in report_dict
		report_dict['report_type'] = report_type
		report_list_id = safe_int(sub_report.get('reportlistid'))
		pattern_type = safe_int(report_dict.get('pattern_type'))
		pattern_time_format = {1: '%H:00', 2: '%a', 3: '%d', 4: '%b'}
		report_dict['pattern_time_format'] = pattern_time_format
		if report_type == 'pattern':
			report_dict['time_format'] = pattern_time_format.get(pattern_type, '%H:00')
			if not pattern_type:
				pattern_type = 1
			report_dict['pattern_type'] = pattern_type
			resolution = getReportParams(report_dict).pattern_resolution_map.get(pattern_type)
			report_dict['resolution'] = resolution
			report_dict['high_resolution'] = resolution
		if not report_dict.has_key('topn'):
			top = safe_int(report_dict.get('report_obj', {}).get('topn', 4))
		else:
			top = safe_int(report_dict.get('topn'))
		report_dict['topn'] = top
		if not report_dict.has_key('timebetween'):
			timebetween = report_dict.get('report_obj', {}).get('timebetween', '')
		else:
			timebetween = report_dict.get('timebetween','')
		report_dict['timebetween'] = timebetween
		if report_type == 'nw_statistic':
			report_dict['t1_start_time'] = report_dict.get('report_obj', {}).get('t1_start_time', '')
			report_dict['t1_end_time'] = report_dict.get('report_obj', {}).get('t1_end_time', '')
			report_dict['t2_start_time'] = report_dict.get('report_obj', {}).get('t2_start_time', '')
			report_dict['t2_end_time'] = report_dict.get('report_obj', {}).get('t2_end_time', '')
		elif hasattr(maxInputStats, 'USE_CUSTOM_TIME_CONFIG') and maxInputStats.USE_CUSTOM_TIME_CONFIG and report_dict.get('report_obj', {}).get('t1_start_time', ''):
			report_dict['st'] = report_dict.get('report_obj', {}).get('t1_start_time', '')
			report_dict['et'] = report_dict.get('report_obj', {}).get('t1_end_time', '')
		
		# take one copy of report dict to the each sub reports
		sub_report_dict = {}
		sub_report_dict.update(report_dict)
		sub_report_dict['report_obj'] = sub_report
		sub_report_dict['report_type'] = report_type
		sub_report_dict['group_type'] = group_type
		sub_report_dict['width'] = safe_int(sub_report.get('width'))
		sub_report_dict['report_list_id'] = report_list_id
		sub_report_dict['display'] = sub_report.get('display', 'avg')
		sub_report_dict['viewtype'] = safe_int(report_dict.get('report_obj', {}).get('viewtype', ''))
		report_dict['sub_report_%d' %(report_list_id)] = sub_report_dict
		return report_dict['sub_report_%d' %(report_list_id)]

	def makeSubGraphs(self, report_dict):
		"""Make graphs for a group.
		"""
		report_type = report_dict.get('report_type')
		if report_type == 'events':
			events = self.getEvents(report_dict)
			return [self.makeEventsTable(events, report_dict)]
		elif report_type == 'statewise_summary':
			statewise_summary =self.vsatStateWiseFilter(report_dict)
			return [self.makeVsatStatewiseTable(statewise_summary, report_dict)]
		elif report_type == 'events_summary':
			max_count = self.getSummaryEvents(report_dict)
			if max_count:
				if safe_int(report_dict.get('format_csv')) and safe_int(report_dict.get('format_csv')) in [1,2] and safe_int(report_dict.get('overview')) == 0:
					events = self.getEvents(report_dict)
					return [self.makeEventsTable(events, report_dict)]
				return [self.drawGraph(report_dict)]
			return [XMLSpan('Data Not Available', classId='maptext')]
		elif report_type == 'down_time': # For Down Time Report
			objs = self.filterAlarms(report_dict)
			if objs :
				return [self.makeDownTimeTable(objs, report_dict)]
			return [XMLSpan('Data Not Available', classId='maptext')]
		elif report_type == 'folder_summary': # For Folder Summary Report
			return [self.makeFolderSummary(report_dict)]
		elif report_type == 'uptime_distribution':
			return [self.makeUptimeDistribution(report_dict)]
		elif report_type == 'downtime_report':
			return [self.makeDownTimeReport(report_dict)]
		elif report_type == 'device_downtime_summary_report':
			return [self.makeDeviceDownTimeSummaryReport(report_dict)]
		elif report_type == 'sla_summary_report':
			return [self.makeSLASummaryReport(report_dict)]
		elif report_type == 'sla_detail_report':
			return [self.makeSLADetailReport(report_dict)]
		#vijay: Integeraed from Everest appliance
		elif report_type == 'node_summary':
			#3.8.2	Node wise Summary Report
			#	This report is based on the nodes.
			return [self.makeNodeWiseSummaryReport(report_dict)]
		elif report_type == 'node_res_summary':
			#3.8.3	Node wise Resource Summary Report
			#	This report is based on the nodes - along with the resources splitted
			return [self.makeNodeWiseResourceSummaryReport(report_dict)]
		elif report_type == 'nodes_summary': # For Nodes Summary Report
			#return [self.makeNodeBasedSummaryTable(report_dict)]
			# For seperating from the call made from makeVSATNetworkEfficiencyTable
			report_dict['report_type_temp'] = 'nodes_summary'
			node_content = self.makeNodeBasedSummaryTable(report_dict)
			#<Robinson Panicker 20120815 : For restricting no of records to be generating based on key MAX_RECORDS_FOR_REPORTS_DISPLAY>
			if type(node_content) == types.DictType and node_content.has_key('maxLimitReached') and node_content.get('maxLimitReached'):
				report_dict['maxLimitReached'] = 1
				contentError = [[XMLText('The result of this report is beyond display capacity. Please reduce the time period for generating the report.'), BR, BR,]]
				logMsg(2,'contentError - %s'%(contentError),'getRFLogs')
				tableHeading = report_dict['report_obj'].get('name', '')
				logMsg(2,'tableHeading - %s'%(tableHeading),'getRFLogs')
				#content = XMLTable(contentError, classId='alignedTableForm')
				content =  XMLContentTable([XMLTable(contentError, classId='alignedTableForm')], table_heading=tableHeading)
				return [content]
			#<Robinson Panicker 20120815 : For restricting no of records to be generating based on key MAX_RECORDS_FOR_REPORTS_DISPLAY ends here>
			if type(node_content) == types.DictType and node_content.has_key('isSplitQuery') and node_content.get('isSplitQuery'):
				#the below condtion is for removing the upper table which is created
				report_dict['isSplitQuery'] = 1
				if report_dict.has_key("fromPDF"):
					return node_content
				#if report_dict.has_key("fromPDF"):
					#return node_content
				elif report_dict.get('splitReportTitle', '') != '':	
					tableHeading = report_dict['report_obj'].get('name', '')
					node_content['title'] = report_dict.get('splitReportTitle', '')
					trafficContent = self.generateSplitQueryPDF(node_content)
					content = XMLContentTable([XMLTable(trafficContent, classId='alignedTableForm')], table_heading=tableHeading)
					return [content]
			else:
				return [node_content]
		elif report_type == 'traps':
			logMsg(2,'In makeSubGrapsh before calling get Traps ','reportpresenter')
			traps = self.getTraps(report_dict)
			logMsg(2,'In makeSubGrapsh completed Traps ','reportpresenter')
			#<Robinson Panicker 20120815 : For restricting no of records to be generating based on key MAX_RECORDS_FOR_REPORTS_DISPLAY>
			if type(traps) == types.DictType and traps.has_key('maxLimitReached') and traps.get('maxLimitReached'):
				report_dict['maxLimitReached'] = 1
				contentError = [[XMLText('The result of this report is beyond display capacity. Please reduce the time period for generating the report.'), BR, BR,]]
				logMsg(2,'contentError - %s'%(contentError),'getTraps')
				tableHeading = report_dict['report_obj'].get('name', '')
				logMsg(2,'tableHeading - %s'%(tableHeading),'getTraps')
				#content = XMLTable(contentError, classId='alignedTableForm')
				content =  XMLContentTable([XMLTable(contentError, classId='alignedTableForm')], table_heading=tableHeading)
				return [content]
			#<Robinson Panicker 20120815 : For restricting no of records to be generating based on key MAX_RECORDS_FOR_REPORTS_DISPLAY ends here>
			if type(traps) == types.DictType and traps.has_key('isSplitQuery') and traps.get('isSplitQuery'):
				report_dict['isSplitQuery'] = 1
				if report_dict.has_key("fromPDF"):
					return traps
				else:	
					if report_dict.get('splitReportTitle', '') != '':
						traps['title'] = report_dict.get('splitReportTitle', '')
					trapContent = self.generateSplitQueryPDF(traps)
					tableHeading = report_dict['report_obj'].get('name', '')
					content = XMLContentTable([XMLTable(trapContent, classId='alignedTableForm')], table_heading=tableHeading)
					return [content]
			else:	
				return [self.makeTrapsTable(traps, report_dict)]
		elif report_type == 'traps_summary':
			traps = self.getTrapsSummary(report_dict)
			#<Robinson Panicker 20120815 : For restricting no of records to be generating based on key MAX_RECORDS_FOR_REPORTS_DISPLAY>
			if type(traps) == types.DictType and traps.has_key('maxLimitReached') and traps.get('maxLimitReached'):
				report_dict['maxLimitReached'] = 1
				contentError = [[XMLText('The result of this report is beyond display capacity. Please reduce the time period for generating the report.'), BR, BR,]]
				tableHeading = report_dict['report_obj'].get('name', '')
				logMsg(2,'tableHeading - %s'%(tableHeading),'getTrapsSummary')
				content =  XMLContentTable([XMLTable(contentError, classId='alignedTableForm')], table_heading=tableHeading)
				return [content]
			#<Robinson Panicker 20120815 : For restricting no of records to be generating based on key MAX_RECORDS_FOR_REPORTS_DISPLAY ends here>
			# updated for the split query for the large chunk of data
			if type(traps) == types.DictType and traps.has_key("isSplitQuery") and traps.get("isSplitQuery", 0) == 1:
				report_dict['isSplitQuery'] = 1
				if report_dict.has_key("fromPDF"):
					return traps
				elif report_dict.get('splitReportTitle', '') != '':
					traps['title'] = report_dict.get('splitReportTitle', '')
					trapContent = self.generateSplitQueryPDF(traps)
					tableHeading = report_dict['report_obj'].get('name', '')
					content = XMLContentTable([XMLTable(trapContent, classId='alignedTableForm')], table_heading=tableHeading)
					return [content]
			else:
				return [self.makeTrapsSummaryTable(traps, report_dict)]
		# For backhaul daily report
		elif report_type == 'backhaul':
			report_dict['report_type_temp'] = 'backhaul'
			backhaul_daily_content = self.makeBackhaulLinkUtilTable(report_dict)
			#<Robinson Panicker 20120815 : For restricting no of records to be generating based on key MAX_RECORDS_FOR_REPORTS_DISPLAY>
			if type(backhaul_daily_content) == types.DictType and backhaul_daily_content.has_key('maxLimitReached') and backhaul_daily_content.get('maxLimitReached'):
				report_dict['maxLimitReached'] = 1
				contentError = [[XMLText('The result of this report is beyond display capacity. Please reduce the time period for generating the report.'), BR, BR,]]
				logMsg(2,'contentError - %s'%(contentError),'getRFLogs')
				tableHeading = report_dict['report_obj'].get('name', '')
				logMsg(2,'tableHeading - %s'%(tableHeading),'getRFLogs')
				#content = XMLTable(contentError, classId='alignedTableForm')
				content =  XMLContentTable([XMLTable(contentError, classId='alignedTableForm')], table_heading=tableHeading)
				return [content]
			#<Robinson Panicker 20120815 : For restricting no of records to be generating based on key MAX_RECORDS_FOR_REPORTS_DISPLAY ends here>
			if type(backhaul_daily_content) == types.DictType and backhaul_daily_content.has_key('isSplitQuery') and backhaul_daily_content.get('isSplitQuery'):
				#the below condtion is for removing the upper table which is created
				report_dict['isSplitQuery'] = 1
				if report_dict.has_key("fromPDF"):
					return backhaul_daily_content
				elif report_dict.get('splitReportTitle', '') != '':
					backhaul_daily_content['title'] = report_dict.get('splitReportTitle', '')
					tableHeading = report_dict['report_obj'].get('name', '')
					backhaulDailyContent = self.generateSplitQueryPDF(backhaul_daily_content)
					content = XMLContentTable([XMLTable(backhaulDailyContent, classId='alignedTableForm')], table_heading=tableHeading)
					return [content]
			else:	
				return [backhaul_daily_content]
		# For backhaul monthly report
		elif report_type == 'backhaul_monthly':
			report_dict['report_type_temp'] = 'backhaul_monthly'
			backhaul_monthly_content = self.makeBackhaulMonthlyTable(report_dict)
			#<Robinson Panicker 20120815 : For restricting no of records to be generating based on key MAX_RECORDS_FOR_REPORTS_DISPLAY>
			if type(backhaul_monthly_content) == types.DictType and backhaul_monthly_content.has_key('maxLimitReached') and backhaul_monthly_content.get('maxLimitReached'):
				report_dict['maxLimitReached'] = 1
				contentError = [[XMLText('The result of this report is beyond display capacity. Please reduce the time period for generating the report.'), BR, BR,]]
				logMsg(2,'contentError - %s'%(contentError),'getRFLogs')
				tableHeading = report_dict['report_obj'].get('name', '')
				logMsg(2,'tableHeading - %s'%(tableHeading),'getRFLogs')
				#content = XMLTable(contentError, classId='alignedTableForm')
				content =  XMLContentTable([XMLTable(contentError, classId='alignedTableForm')], table_heading=tableHeading)
				return [content]
			#<Robinson Panicker 20120815 : For restricting no of records to be generating based on key MAX_RECORDS_FOR_REPORTS_DISPLAY ends here>
			if type(backhaul_monthly_content) == types.DictType and backhaul_monthly_content.has_key('isSplitQuery') and backhaul_monthly_content.get('isSplitQuery'):
				#the below condtion is for removing the upper table which is created
				report_dict['isSplitQuery'] = 1
				if report_dict.has_key("fromPDF"):
					return backhaul_monthly_content
				elif report_dict.get('splitReportTitle', '') != '':
					backhaul_monthly_content['title'] = report_dict.get('splitReportTitle', '')
					tableHeading = report_dict['report_obj'].get('name', '')
					backhaulDailyContent = self.generateSplitQueryPDF(backhaul_monthly_content)
					content = XMLContentTable([XMLTable(backhaulDailyContent, classId='alignedTableForm')], table_heading=tableHeading)
					return [content]
			else:
				return [backhaul_monthly_content]
		elif report_type == 'uptime_summary':
			return [self.makeUptimeSummaryTable(report_dict)]
		elif report_type == 'nw_statistic': # For VSAT Platform wise Summary Report
			return [self.makeVSATNetworkStatisticTable(report_dict)]
		elif report_type == 'nw_efficiency':
			return [self.makeVSATNetworkEfficiencyTable(report_dict)]
		elif (report_dict.get('resolution','raw') == 'day' or report_dict.get('resolution','raw') == 'month') and safe_int(report_dict.get('business_hr','0')) != 0 :
			return [XMLSpan('Data not available in Day/Month resolution for Business Hour / Non Business Hour Configuration', classId='maptext')]
		else:
			group_reports = []
			group_type = safe_int(report_dict.get('group_type'))
			if report_type in ['downtime_report','uptime_summary','backhaul', 'backhaul_monthly']:
				topn = safe_int(report_dict.get('topn'))
				if topn:
					# reset the topn value to 0 so that it will get the value for the selected stats for all the resources
					# for this report, the topn is applicable for the down time calculations.
					report_dict['top_value'] = topn
					report_dict['topn'] = 0
			if report_type.find('mrtg') !=-1:
				if report_dict['report_obj']['statids'] not in [[145, 146],[146, 145],[145],[146]]:
					return [XMLSpan('Configured Statistics not supported for MRTG Graph', classId='maptext')]
				else:
					group_type=1
					report_dict['group_type'] = 1 
					report_dict['topn']=1
					time_between = report_dict['report_obj'].get('timebetween','')
					custom_time_diff=report_dict['et'] - report_dict['st']
					purge_time_diff = safe_int(time.time())-report_dict['st']
					purge_value=purge_time_diff/86400
					#print purge_value
					raw_limit,hour_limit,day_limit,month_limit=self.purge_dict.get('tblRaw',3),self.purge_dict.get('tblHour',90),self.purge_dict.get('tblDay',24),self.purge_dict.get('tblMonth',2)					
					month=safe_int(time.strftime('%m', time.localtime(safe_int(time.time()))))
					year=safe_int(time.strftime('%Y', time.localtime(safe_int(time.time()))))
					no_of_days=getReportParams(report_dict).monthToDayDict.get(month, 31)
					days_in_year=365
					if year % 4 == 0 and month == 2 :
						# Increasing Feb Days to 29 for Leap Yrs
						no_of_days = no_of_days + 1
						days_in_year=366
					if report_type == 'mrtg_daily':
						report_dict['resolution']='raw'
						report_dict['high_resolution']='month'
						report_dict['raw_resolution_aggregation'] = 60
						if ((report_dict.get('cal_val','')!='') or (report_dict.get('start_time','')!='')):
							if purge_value>raw_limit:
								report_dict['resolution']='hour'
							elif purge_value>hour_limit:
								report_dict['resolution']='day'
							elif purge_value>(day_limit/12)*365:
								report_dict['resolution']='month'
						timescale=7
                				report_dict['timescale']=7
						if ((report_dict.get('cal_val','')=='') and (report_dict.get('start_time','')=='')):
							report_dict['et']=safe_int(time.time())
							report_dict['st']=safe_int(report_dict['et'] - 3600*24)#3600*24=86400=1day
						elif ((report_dict.get('start_time','')!='') and (custom_time_diff > 86400)):
							return [XMLSpan('Data Not Available', classId='maptext')]
						end_duration=time.strftime('%d-%b-%Y %H:%M', time.localtime(safe_int(time.time())))
						start_duration=time.strftime('%d-%b-%Y %H:%M', time.localtime(safe_int(safe_int(time.time()) - 3600*24)))
					elif report_type == 'mrtg_weekly':
						report_dict['resolution']='raw'
						report_dict['high_resolution']='month'
						report_dict['raw_resolution_aggregation'] = 60
						if ((report_dict.get('cal_val','')!='') or (report_dict.get('start_time','')!='')):
							if purge_value>raw_limit:
								report_dict['resolution']='hour'
							elif purge_value>hour_limit:
								report_dict['resolution']='day'
							elif purge_value>(day_limit/12)*365:
								report_dict['resolution']='month'
						timescale=10
                				report_dict['timescale']=10
                				if ((report_dict.get('cal_val','')=='') and (report_dict.get('start_time','')=='')):
							report_dict['et']=safe_int(time.time())
							report_dict['st']=safe_int(report_dict['et'] - 86400*7)
						elif (((report_dict.get('start_time','')!='') and (custom_time_diff not in range(86401,(86400*7)+1))) or (report_dict.get('cal_val','')!='')):
							return [XMLSpan('Data Not Available', classId='maptext')]
						end_duration=time.strftime('%d-%b-%Y %H:%M', time.localtime(safe_int(time.time())))
						start_duration=time.strftime('%d-%b-%Y %H:%M', time.localtime(safe_int(safe_int(time.time()) - 86400*7)))
					elif report_type == 'mrtg_monthly':
						report_dict['resolution']='hour'
						report_dict['high_resolution']='month'
						if ((report_dict.get('cal_val','')!='') or (report_dict.get('start_time','')!='')):					
							if purge_value>hour_limit:
								report_dict['resolution']='day'
							elif purge_value>(day_limit/12)*365:
								report_dict['resolution']='month'
						timescale=13
                				report_dict['timescale']=13
                				if ((report_dict.get('cal_val','')=='') and (report_dict.get('start_time','')=='')):
							report_dict['et']=safe_int(time.time())
							report_dict['st']=safe_int(report_dict['et'] - 86400*no_of_days)
						elif (((report_dict.get('start_time','')!='') and (custom_time_diff not in range((86400*7)+1,(86400*no_of_days)+1))) or (report_dict.get('cal_val','')!='')):
							return [XMLSpan('Data Not Available', classId='maptext')]
						end_duration=time.strftime('%d-%b-%Y %H:00', time.localtime(safe_int(time.time())))
						start_duration=time.strftime('%d-%b-%Y %H:00', time.localtime(safe_int(safe_int(time.time()) - 86400*no_of_days)))
					elif report_type == 'mrtg_yearly':
						report_dict['resolution']='day'
						report_dict['high_resolution']='month'
						if ((report_dict.get('cal_val','')!='') or (report_dict.get('start_time','')!='')):					
							if purge_value>(day_limit/12)*365:
								report_dict['resolution']='month'
						timescale= 18 
                				report_dict['timescale']=18
       						if ((report_dict.get('cal_val','')=='') and (report_dict.get('start_time','')=='')):
							report_dict['et']=safe_int(time.time())
							report_dict['st']=safe_int(report_dict['et'] - 86400*days_in_year)
						elif (((report_dict.get('start_time','')!='') and (custom_time_diff < (86400*no_of_days)+1)) or (report_dict.get('cal_val','')!='')):
							return [XMLSpan('Data Not Available', classId='maptext')]
						end_duration=time.strftime('%d-%b-%Y 00:00', time.localtime(safe_int(time.time())))
						start_duration=time.strftime('%d-%b-%Y 00:00', time.localtime(safe_int(safe_int(time.time()) - 86400*days_in_year)))
					t_period = '%s - %s %s (%s)' %(start_duration, end_duration,time_between, getTimeZone())	
					report_dict['mrtg_time_period_heading']=t_period
			self.getGroupReportDatas(report_dict, group_type)
			sub_report_data_dict = report_dict['sub_report_data_dict']
			if sub_report_data_dict:
				group_legend_unit_map = report_dict.get('legend_unit_map')
				temp_datas = self.getSortedDatas(sub_report_data_dict, report_dict)
				for groupid, report_data_dict in temp_datas:
					if report_type == 'backhaul' and  groupid == self.rawdn_id_map.get('no_of_vsats_up') and group_type == 0:
						continue
					method = getReportParams(report_dict).report_class_map.get(report_type)
					if method == TrendChart and safe_int(report_dict.get('month_graph')):
						if report_dict.get('resolution','raw') == 'day':
							#Extrapolate a value for the last entry
							increment = 3600 * 24
							logMsg(2,'Extrapolating the graph point - increment by %s value for last value'%increment,self.module_name)
							for key,value in report_data_dict.items():
								value.append((value[-1][0] + (increment),NULL_VALUE))
					legend_unit_map = group_legend_unit_map.get(groupid)
					self.checkForNullValues(groupid, report_data_dict, report_dict)
					y_title = report_dict.get('title_map', {}).get(groupid, {}).get('y_title')
					chart_title = report_dict.get('title_map', {}).get(groupid, {}).get('chart_title')
					report_dict['out'] = {'y_title': y_title, 'chart_title': chart_title}
					#report_dict['out'] = {'y_title': report_dict.get('title_map', {}).get(groupid, '')}
					self.getDataParams(groupid, report_data_dict, legend_unit_map, report_dict)
					report_dict["groupid"] = groupid
					if safe_int(report_dict.get('fromMobile')):
						group_reports.append(report_dict.get('out', {}))
					else:
						group_report = self.drawGraph(report_dict)
						if type(group_report) == type([]):
							group_reports += group_report
						else:	
							group_reports.append(group_report)
				return group_reports
			return [XMLSpan('Data Not Available', classId='maptext')]
	
	#Added by Divyanka For Vsat Statewise Summary Report Configuration
	def vsatStateWiseFilter(self,report_dict):
		final_list = []
		try:
			starttime = report_dict.get('st')
			endtime = report_dict.get('et')
			event_tables = self.event_db.getTables(starttime, endtime)
			event_tables.sort()
			event_tables.reverse()
			resids_total = report_dict.get('res_hostname_map', {}).keys()
			logMsg(4, "Events Tables : %s, Number of Resids : %s"%(event_tables, len(resids_total)), self.module_name)
			if event_tables and resids_total:
				for event_table in event_tables:
					sql_stmt = "select * from (select r.resid, r.name, e.severity, e.msg, n.syslocation, ex.conperson, ex.address, e.up_down, e.timeStamp, RANK() OVER (PARTITION BY r.resid ORDER BY e.timeStamp desc) from %s e, tblResconfig r, tblThresholds t, tblNodeInfo n , tblExtraNodeInfo ex where (e.timeStamp between %s and %s) and r.resid in (%s) and r.resid = t.resid and t.thresid = e.fid and r.name = n.hostname and n.nodeid = ex.nodeid) as tmp where tmp.rank = 1 order by timeStamp desc"%(event_table, starttime, endtime, str(resids_total)[1:-1])
					logMsg(4, "Query Shyam----------------------- : %s"%(sql_stmt), self.module_name)
					objs = DatabaseServer.executeSQLStatement(sql_stmt)
					if objs and type(objs) == type([]):
						tmp_resids = map(lambda a: a.get('resid'), objs)
						resids_total = filter(lambda a: a not in tmp_resids, resids_total)
						final_list = final_list + objs
					if not resids_total:
						break #got all Events
		except Exception, msg:
			logExceptionMsg(4, "Exception in vsatStateWiseFilter %s"%msg, self.module_name)
		return final_list
		
	def getEvents(self, report_dict):
		"""Get Events from the database for the configured path.
		"""
		db_type = getDBType()
		DatabaseServer = getDBInstance()
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		top = safe_int(report_dict.get('topn'))
		event_tables = self.event_db.getTables(start_time, end_time)
		resids = report_dict.get('res_path_map', {}).keys()
		stats = report_dict.get('report_obj', {}).get('statids', [])
		report_dict['statids'] = stats
		time_filter = timeBetween(start_time, end_time, report_dict.get('timebetween',''))
		top_str = ''
		if top:
			if db_type == 'MySQL' or db_type == 'PostgreSQL':
				top_str = ' LIMIT %s' %(top)
			elif db_type == 'MSSQL':
				top_str = ' TOP %s ' %(top)
			elif db_type == 'ORACLE':
				top_str = ' AND ROWNUM <= %s ' %(top)
		events = []
		logMsg(2, 'top - %s and top_str - %s' %(top, top_str), self.module_name)
		for event_table in event_tables:
			if db_type == 'MySQL' or db_type == 'PostgreSQL':
				#sql_stmt = 'SELECT t.resid, e.timeStamp, t.statid, IF(e.up_down = 0, t.severity, 1) severity, IF(e.up_down = 0, t.setMsg, t.resetMsg) msg FROM %s e, tblThresholds t WHERE e.fid = t.thresid AND t.resid IN (%s) AND t.statid IN (%s) AND e.timeStamp BETWEEN %s AND %s AND t.isDeleted = 0 ORDER BY e.timeStamp DESC %s'
				if top:
					sql_stmt = queryStandardizer.getQuery('Q658') + top_str
					sql_stmt = sql_stmt%(event_table, str(resids)[1:-1], str(stats)[1:-1], start_time, end_time)
				else:
					sql_stmt = queryStandardizer.getQuery('Q658') %(event_table, str(resids)[1:-1], str(stats)[1:-1], start_time, end_time)

			elif db_type == 'MSSQL':
				#sql_stmt = 'SELECT %s t.resid, e.timeStamp, t.statid, (CASE WHEN e.up_down=0 THEN t.severity ELSE 1 END) severity, (CASE WHEN e.up_down=0 THEN t.setMsg ELSE t.resetMsg END) msg FROM %s e, tblThresholds t WHERE e.fid = t.thresid AND t.resid IN (%s) AND t.statid IN (%s) AND e.timeStamp BETWEEN %s AND %s AND t.isDeleted = 0 ORDER BY e.timeStamp DESC'
				sql_stmt = queryStandardizer.getQuery('Q658') %(top_str, event_table, str(resids)[1:-1], str(stats)[1:-1], start_time, end_time, time_filter)

			# Need to check the query for Oracle
			elif db_type == 'ORACLE':
				#sql_stmt = 'SELECT t.resid, e.timeStamp, t.statid, CASE WHEN e.up_down=0 THEN t.severity ELSE 1 severity, CASE WHEN e.up_down=0 THEN t.setMsg ELSE t.resetMsg msg FROM %s e, tblThresholds t WHERE e.fid = t.thresid AND t.resid IN (%s) AND t.statid IN (%s) AND e.timeStamp BETWEEN %s AND %s AND t.isDeleted = 0 %s ORDER BY e.timeStamp DESC'
				sql_stmt = queryStandardizer.getQuery('Q658') %(event_table, str(resids)[1:-1], str(stats)[1:-1], start_time, end_time, top_str)
			logMsg(2, 'sql_stmt - %s' %(sql_stmt), self.module_name)
			objs = DatabaseServer.executeSQLStatement(sql_stmt)
			logMsg(2, 'objs -- %s' %(len(objs)), self.module_name)
			if objs != [] and objs != -1:
				events += objs
			if top and len(events) >= top:
				break
		if top:
			events = events[:top]
		logMsg(2, 'Number of events -- %s' %(len(events)), self.module_name)
		self.updateGroupInfo(report_dict)
		return events
		# Added By Divyanka For Vsat Statewise Report

	def makeVsatStatewiseTable(self, events, report_dict):
		severityMap = {
				0: 'images/alarm_indicator_1.gif', 1: 'images/alarm_indicator_2.gif', 
				2: 'images/alarm_indicator_2.gif', 3: 'images/alarm_indicator_3.gif', 
				4: 'images/alarm_indicator_3.gif', 5 : 'images/alarm_indicator_4.gif', 
				6: 'images/alarm_indicator_5.gif',
			}
		headings = [
				('name', 'VSAT ID', '10%', 'left'), 
				('_severity', 'Severity', '5%', 'left'), 
				('_msg', 'Status', '15%', 'left'),
				('timestamp', 'Time', '12%', 'left'), 
				('conperson', 'Customer Name', '15%', 'left'), 
				('_address', 'Location', '40%', 'left'), 
			]
		csv_headings = [
				('name', 'VSAT ID', 25), 
				('severity', 'Severity', 5), 
				('msg', 'Status', 15),
				('_timestamp', 'Time', 12), 
				('conperson', 'Customer Name', 15), 
				('address', 'Location', 40), 
			]
		
		objs = []
		required_images = []
		for event in events:
			# For HTML Online Report
			severity_image = severityMap.get(safe_int(event.get('severity')), 1)
			severity_image = severity_image.replace('/', '\\\\')
			event['_severity'] = XMLImage(severity_image)
			# For HTML Offline Report
			if severity_image not in required_images:
				required_images.append(severity_image)
			event['_msg'] = XMLNOBR(event.get('msg'))
			event['_address'] = XMLNOBR(event.get('address'))
			lt = time.localtime(safe_int(event.get('timestamp', event.get('timeStamp'))))
			# For HTML Report
			event['_timestamp'] = time.strftime('%m/%d/%Y %H:%M:%S', lt)
			event['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', lt)
			objs.append(event)
		# For HTML Offline Report
		required_images = unique(required_images)
		if required_images and safe_int(report_dict.get('html_offline_report')):
			if not report_dict.has_key('created_charts'):
				report_dict['created_charts'] = []
			report_dict['created_charts'].extend(required_images)
		width = report_dict.get('width', 420)
		if width > 420:
			if len(objs) < 15:
				height = (len(objs) * 16) + 50
			else:
				height = 280
		else:
			height = 280
			if len(objs) < 15:
				self.addDummyObjs(headings, objs)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY :
			report_dict['no_resource'] = len(objs)
			#headings = csv_headings
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)

	def makeEventsTable(self, events, report_dict):
		"""Make Events Table
		"""
		severityMap = {
				0: 'images/alarm_indicator_1.gif', 1: 'images/alarm_indicator_2.gif', 
				2: 'images/alarm_indicator_2.gif', 3: 'images/alarm_indicator_3.gif', 
				4: 'images/alarm_indicator_3.gif', 5 : 'images/alarm_indicator_4.gif', 
				6: 'images/alarm_indicator_5.gif',
			}
		res_path_map = report_dict.get('res_path_map')
		headings = [('severity', 'Status', '5%', 'center'), ('timeStamp', 'TimeStamp', '20%', 'center'), ('path', 'Resource', '45%', 'left'), ('msg', 'Description', '30%', 'left')]
		csv_headings = [
				('_severity', 'Severity', 25), 
				('_timeStamp', 'TimeStamp', 15), 
				('_path', 'Resource', 50), 
				('_msg', 'Event Description', 50),
			]
		severityNameMap = {1: 'Information', 3: 'Warning', 5: 'Error', 6: 'Serious Error'}			
		objs = []
		for event in events:
			path = res_path_map.get(safe_int(event.get('resid')))
			stat_name = changeOption(self.statid_name_map.get(safe_int(event.get('statid'))))
			#path_name = '%s (%s)' %(stat_name, path)
			path_name = path
			msg = event.get('msg', '')
			obj = {}
			obj['severity'] = severityNameMap.get(safe_int(event.get('severity')), 'Error')
			obj['_severity'] = severityNameMap.get(safe_int(event.get('severity')), 'Error')
			#obj['_severity'] = safe_int(event.get('severity'))
			lt = time.localtime(safe_int(event.get('timeStamp')))
			#bug fix 3602
			if report_dict['request'].get('fromPDF'):
				obj['_timeStamp'] = DateTime.DateTime(lt[0], lt[1], lt[2], lt[3], lt[4], lt[5])
				#obj['severity'] = XMLImage(severityMap.get(safe_int(event.get('severity')), 1))
				obj['timeStamp'] = time.strftime('%d-%b-%Y %H:%M:%S', time.localtime(safe_int(event.get('timeStamp'))))
				obj['path'] = path_name
				obj['msg'] = msg
				obj['_path'] = path_name
				obj['_msg'] = msg
			else:
				obj['_timeStamp'] = DateTime.DateTime(lt[0], lt[1], lt[2], lt[3], lt[4], lt[5])
				obj['severity'] = XMLImage(severityMap.get(safe_int(event.get('severity')), 1))
				obj['timeStamp'] = time.strftime('%d-%b-%Y %H:%M:%S', time.localtime(safe_int(event.get('timeStamp'))))
				obj['path'] = XMLSpan(path_name, title=path_name)
				obj['msg'] = XMLSpan(msg, title=msg)
				obj['_path'] = path_name
				obj['_msg'] = msg
			objs.append(obj)
		width = report_dict.get('width', SMALL_REPORT_WIDTH)
		if width > SMALL_REPORT_WIDTH:
			if len(objs) < 15:
				height = (len(objs) * 16) + 50
			else:
				height = 280
		else:
			height = 280
			if len(objs) < 15 and safe_int(report_dict.get("fromPDF",0)):
				self.addDummyObjs(headings, objs)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY :
			report_dict['no_resource'] = len(objs)				
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)

	def getSummaryEvents(self, report_dict):
		"""Get summary events
		"""
		events = self.getEvents(report_dict)
		severityMap = {1: 1, 2: 1, 3: 3, 4: 3, 5: 5, 6: 6}
		severityNameMap = {1: 'Information', 3: 'Warning', 5: 'Error', 6: 'Serious Error'}
		severity_count = {1:0, 3:0, 5:0, 6:0}
		for event in events:
			severity = safe_int(event.get('severity'))
			severity = severityMap.get(severity, severity)
			if not severity_count.has_key(severity):
				severity_count[severity] = 0
			severity_count[severity] += 1
		datas = []
		temp_datas = severity_count.items()
		temp_datas.sort(sortTuple1)
		max_count = 0
		for severity, count in temp_datas:
			datas.append((severityNameMap.get(severity, 'Information'), count))
			if count > max_count:
				max_count = count
		max_labels = (safe_int(max_count / 10) + 1) * 10
		report_dict['out'] = {'y_title': '', 'max_labels': max_labels}
		report_dict['division_factor'] = 1
		report_dict['out']['data'] = datas
		return max_count
		
	def makeNodeWiseSummaryReport(self,report_dict):
		"""Return the node wise summary report: Looks like below
		"""
		self.updateGroupInfo(report_dict)
		headings = [('host_name', 'Node', '20%',"left"),('ip_address','IP Address', '20%', "left")
			#Add the rest of the statistics from the database.
		]
		csv_headings = [('host_name', 'Node', 75), ('ip_address','IP Address', 75)]
		#Find out all the resources which are used for generating the reports
		host_ip_average = {}
		group_report = []
		report_dict['statids_backup'] = report_dict.get('statids')
		report_dict['topn_backup'] = report_dict.get('topn',0)
		report_dict['topn'] = 0			#We take the summary of the Resources then filter the no of nodes displayed.
		report_dict['report_type'] = 'summary'	#ofcourse you need summary data right. ???
		#Replace the group type to 'resource split'
		report_dict['group_type_backup'] = safe_int(report_dict.get('group_type',1))
		report_dict['group_type'] = 1		#I need only resource split for generating the report
		self.getGroupReportDatas(report_dict, report_dict.get('group_type',1))
		sub_report_data_dict = report_dict['sub_report_data_dict']
		# Ramaraja - To maintain the heading order 
		order_map = {}
		for statid in report_dict.get('statids',[]):
			statname = self.statid_name_map.get(statid)
			order_map.update({statname:statid})
		ordered_list = []
		for each_statname in report_dict.get('report_obj').get('stats','').split('|'):
			each_statname = each_statname.strip()
			ordered_list.append(order_map.get(each_statname))
		report_dict['statids'] = ordered_list
		if sub_report_data_dict:
			stat_name_unit_map = {}
			group_legend_unit_map = report_dict.get('legend_unit_map')
			temp_datas = self.getSortedDatas(sub_report_data_dict, report_dict)
			for groupid, report_data_dict in temp_datas:
				#Get the legend unit maps and create stat_name_unit_map for unit mapping.
				legend_unit_map = group_legend_unit_map.get(groupid)
				stat_name_unit_map.update(legend_unit_map)
				#Add the following resources into the datbase
				host_ip_key = (report_dict.get('res_hostname_map',{}).get(groupid),report_dict.get('res_hostip_map',{}).get(groupid))	#used for sorting and all.
				if not host_ip_average.has_key(host_ip_key):
					#Group id is resource /  for any resource split of entires.
					#host_ip_average.update({host_ip_key:{'host_name':host_ip_key[0],'ip_address':[1]}})
					host_ip_average.update({host_ip_key:{}})
				#resulting dataset 
				#temp_data_set = report_data_dict.get()
				for stat_name,value in report_data_dict.items():
					#update the result set
					logMsg(2, 'stat_name -- %s'%stat_name, self.module_name)
					if not host_ip_average.get(host_ip_key,{}).has_key(stat_name):
						host_ip_average.get(host_ip_key,{}).update({stat_name:[]})
					host_ip_average.get(host_ip_key,{}).get(stat_name).extend(value)
					#update the stat for the later use
			#Make the average of all the resources
			for host_ip_key,dataset in host_ip_average.items():
				for stat_name,valuelist in dataset.items():
					#Except ip and host name - rest every thing is a statistics.
					#Make the average of all the data set.
					if stat_name in ('Reboot Counter'):
						avg_val = sum(map(lambda a: a[1],valuelist))
					else:						
						avg_val = avg(map(lambda a: a[1],valuelist))
					host_ip_average[host_ip_key][stat_name] = formatValue(avg_val,stat_name_unit_map.get(stat_name))
					if self.statid_name_map.get(report_dict.get('statids',[])[0]) == stat_name:
						host_ip_average[host_ip_key]['first_stat_avg'] = avg_val
				host_ip_average[host_ip_key]["host_name"] = host_ip_key[0]
				if not host_ip_average[host_ip_key].get('first_stat_avg'):
					host_ip_average[host_ip_key]['first_stat_avg'] = None		#used for sorting :)
				#We need an adhoc report :)
				filters = 'statids=%s&poll_addr=%s%s&topn=%s' %(report_dict.get('statids',[]), host_ip_key[1], self.getSelectedTime(report_dict.get('request')),report_dict.get('topn'))
				report_link = 'javascript:openAdHocReport(&quot;selectgroupreport?%s&group_type=0&report_type=trend&quot;);' %(filters)
				if report_dict.get('fromPDF',0):
					host_ip_average[host_ip_key]["ip_address"] = host_ip_key[1]	#Just the IP Address
				else:
					host_ip_average[host_ip_key]["ip_address"] = XMLLink(report_link, host_ip_key[1], Title=host_ip_key[1], classId='commons1')
			#Update the headings here
			stat_list = []
			if report_dict.get('report_obj').get('stats',''):
				stat_list = report_dict.get('report_obj').get('stats','').split('|')
				for statname in stat_list:
					statname = statname.strip()
					headings.append((statname,statname,'15%',"left"))
					csv_headings.append((statname,statname,25))
			else:
				for statid in eval(report_dict.get('statids_in_order',report_dict.get('statids',[]))):
					statname = self.statid_name_map.get(safe_int(statid))
					headings.append((statname,statname,'15%',"left"))
					csv_headings.append((statname,statname,25))
			#Sorting for the data is required 
			#	- Sort by the first statistics[Data is already formatted so..]
			ret_data = host_ip_average.values()
			#sort by descending order
			sortCriteria = self.parseSortBy('-first_stat_avg')
			ret_data.sort(lambda a, b, c = sortCriteria : cmpField(a, b, c))
			#Get the topn Nodes
			if safe_int(report_dict.get('topn_backup',0)):
				ret_data = ret_data[0:safe_int(report_dict.get('topn_backup',0))]
			report_dict['topn'] = report_dict['topn_backup']
			report_dict['report_type'] = 'node_summary'	#Back on track
			report_dict['statids'] = report_dict.get('statids_backup')
			self.updateGroupInfo(report_dict)
			return XMLReportDataAndSummaryTable(headings,objs=ret_data,font_size=report_dict.get('font', 's'), data=1, width=report_dict['width'],csv_schema=csv_headings)
		report_dict['report_type'] = 'node_summary'	#ofcourse you need to reset to node_summary right. ???
		report_dict['topn'] = report_dict['topn_backup']
		report_dict['group_type'] = report_dict['group_type_backup']
		report_dict['statids'] = report_dict.get('statids_backup')
		group_report = XMLSpan('Data Not Available', classId='maptext')
		#If in case it comes from the overview.
		if not sub_report_data_dict and report_dict.get('overview'):
			group_report = []
		self.updateGroupInfo(report_dict)
		return group_report
		
	def makeNodeWiseResourceSummaryReport(self,report_dict):
		"""Return the node wise summary report.
		"""
		self.updateGroupInfo(report_dict)
		headings = [('host_name', 'Node', '20%',"left"),('ip_address','IP Address', '20%', "left"),('resource_name','Resource', '20%', "left")
			#Add the rest of the statistics from the database.
		]
		csv_headings = [('host_name', 'Node', 30), ('ip_address','IP Address', 30),('resource_name','Resource', 50)]
		#Find out all the resources which are used for generating the reports
		host_ip_average = {}
		group_report = []
		report_dict['statids_backup'] = report_dict.get('statids')
		report_dict['topn_backup'] = report_dict.get('topn',0)
		report_dict['topn'] = 0			#We take the summary of the Resources then filter the no of nodes displayed.
		report_dict['report_type'] = 'summary'	#ofcourse you need summary data right. ???
		#Replace the group type to 'resource split'
		report_dict['group_type_backup'] = safe_int(report_dict.get('group_type',1))
		report_dict['group_type'] = 1		#I need only resource split for generating the report
		self.getGroupReportDatas(report_dict, report_dict.get('group_type',1))
		sub_report_data_dict = report_dict['sub_report_data_dict']
		#Ramaraja - For proper column order 
		order_map = {}
		for statid in report_dict.get('statids',[]):
			statname = self.statid_name_map.get(statid)
			order_map.update({statname:statid})
		ordered_list = []
		for each_statname in report_dict.get('report_obj').get('stats','').split('|'):
			each_statname = each_statname.strip()
			ordered_list.append(order_map.get(each_statname))
		report_dict['statids'] = ordered_list
		if sub_report_data_dict:
			stat_name_unit_map = {}
			group_legend_unit_map = report_dict.get('legend_unit_map')
			temp_datas = self.getSortedDatas(sub_report_data_dict, report_dict)
			for groupid, report_data_dict in temp_datas:
				#Get the legend unit maps and create stat_name_unit_map for unit mapping.
				legend_unit_map = group_legend_unit_map.get(groupid)
				stat_name_unit_map.update(legend_unit_map)
				#Add the following resources into the datbase
				host_ip_key = (report_dict.get('res_hostname_map',{}).get(groupid),report_dict.get('res_hostip_map',{}).get(groupid))	#used for sorting and all.
				if not host_ip_average.has_key(host_ip_key):
					#Group id is resource /  for any resource split of entires.
					#host_ip_average.update({host_ip_key:{'host_name':host_ip_key[0],'ip_address':[1]}})
					host_ip_average.update({host_ip_key:{}})
				#update the result set
				host_ip_average.get(host_ip_key,{}).update({groupid:report_data_dict})
				#update the stat for the later use
			#First statistics for the sorting
			first_stat_name = self.statid_name_map.get(report_dict.get('statids',[])[0])
			#Make the average of all the data set for each resources
			for host_ip_key,dataset in host_ip_average.items():
				for groupid,report_data_dict in dataset.items():
					#Make the average of all the data set for individual statistics
					for stat_name,valuelist in report_data_dict.items():
						if stat_name in ('DownTime', 'Business Downtime', 'UnknownTime', 'Business Unknown Time') :
							avg_val = sum(map(lambda a: a[1],valuelist))
							avg_val = makeTimeDelta(avg_val)
						elif stat_name in ("Bytes Sent", "Bytes Received","Reboot Counter"):
							avg_val = sum(map(lambda a: a[1],valuelist))
						else:
							avg_val = avg(map(lambda a: a[1],valuelist))
						host_ip_average[host_ip_key][groupid][stat_name] = formatValue(avg_val,stat_name_unit_map.get(stat_name))
						if self.statid_name_map.get(report_dict.get('statids',[])[0]) == stat_name:
							host_ip_average[host_ip_key][groupid]['first_stat_avg'] = avg_val	#used for sorting :)
					host_ip_average[host_ip_key][groupid]["host_name"] = host_ip_key[0]
					if not host_ip_average[host_ip_key][groupid].has_key('first_stat_avg'):
						host_ip_average[host_ip_key][groupid]['first_stat_avg'] = None		#used for sorting :)
					#We need an adhoc report :)
					filters = 'statids=%s&poll_addr=%s%s&topn=%s' %(report_dict.get('statids',[]), host_ip_key[1], self.getSelectedTime(report_dict.get('request')),report_dict.get('topn'))
					report_link = 'javascript:openAdHocReport(&quot;selectgroupreport?%s&group_type=0&report_type=trend&quot;);' %(filters)
					if report_dict.get('fromPDF',0):
						host_ip_average[host_ip_key][groupid]["ip_address"] = host_ip_key[1]	#Just the IP Address
						host_ip_average[host_ip_key][groupid]['resource_name'] = str(report_dict.get('res_path_map',{}).get(groupid))[:50]
						if report_dict.get('res_path_map',{}).get(groupid).find('/') != -1:
							host_ip_average[host_ip_key][groupid]['resource_name'] = report_dict.get('res_path_map',{}).get(groupid).split('/')[-1][:50]
					else:
						#Link for the host names
						host_ip_average[host_ip_key][groupid]["ip_address"] = XMLLink(report_link, host_ip_key[1], Title=host_ip_key[1], classId='commons1')
						#Link for the resources.
						filters = 'statids=%s&poll_addr=%s%s&topn=%s&pathnames=%s' %(report_dict.get('statids',[]), host_ip_key[1], self.getSelectedTime(report_dict.get('request')),report_dict.get('topn'),report_dict.get('res_actual_path_map',{}).get(groupid))
						report_link = 'javascript:openAdHocReport(&quot;selectgroupreport?%s&group_type=0&report_type=trend&quot;);' %(filters)
						#host_ip_average[host_ip_key][groupid]['resource_name'] = str(report_dict.get('res_path_map',{}).get(groupid))[:50]
						host_ip_average[host_ip_key][groupid]['resource_name'] = XMLLink(report_link, str(report_dict.get('res_path_map',{}).get(groupid))[:50], Title=str(report_dict.get('res_path_map',{}).get(groupid))[:50], classId='commons1')
						if report_dict.get('res_path_map',{}).get(groupid).find('/') != -1:
							host_ip_average[host_ip_key][groupid]['resource_name'] = XMLLink(report_link, report_dict.get('res_path_map',{}).get(groupid).split('/')[-1][:50], Title=report_dict.get('res_path_map',{}).get(groupid).split('/')[-1][:50], classId='commons1')
							#host_ip_average[host_ip_key][groupid]['resource_name'] = report_dict.get('res_path_map',{}).get(groupid).split('/')[-1][:50]
			#Update the headings here
			#stat_list = report_dict.get('report_obj').get('stats','').split('|')
			stat_list = []
			if report_dict.get('report_obj').get('stats',''):
				stat_list = report_dict.get('report_obj').get('stats','').split('|')
				for statname in stat_list:
					statname = statname.strip()
					headings.append((statname,statname,'15%',"left"))
					csv_headings.append((statname,statname,25))
			else:
				for statid in eval(report_dict.get('statids_in_order',report_dict.get('statids',[]))):
					statname = self.statid_name_map.get(safe_int(statid))
					headings.append((statname,statname,'15%',"left"))
					csv_headings.append((statname,statname,25))
			
			#Sorting for the data is required 
			#	- Sort by the first_stat_avg statistics[Data is already formatted so :( ..]
			host_ip_aver_sorted = host_ip_average.keys()
			host_ip_aver_sorted.sort(sortTuple1)
			#host_ip_aver_sorted.reverse()
			result = []
			count = 0
			for host_ip_key in host_ip_aver_sorted: 
				if safe_int(report_dict.get('topn_backup',0)) != 0 and safe_int(report_dict.get('topn_backup',0)) <= count:
						break	#Its enough entries.
				count = count + 1
				data_set = host_ip_average[host_ip_key]
				ret_data = data_set.values()
				#sort by descending order
				sortCriteria = self.parseSortBy('-first_stat_avg')
				ret_data.sort(lambda a, b, c = sortCriteria : cmpField(a, b, c))
				result += ret_data
				#Cleaning needs to be done for the ui
				for each_ret_data in ret_data[1:]:
					if each_ret_data.has_key('ip_address'):
						each_ret_data['ip_address'] = ' '
					if each_ret_data.has_key('host_name'):
						each_ret_data['host_name'] = ' '
			#Get the topn Nodes
			report_dict['topn'] = report_dict['topn_backup']
			report_dict['report_type'] = 'node_res_summary'	#Back on track
			report_dict['statids'] = report_dict.get('statids_backup')
			self.updateGroupInfo(report_dict)
			return XMLReportDataAndSummaryTable(headings,objs=result,font_size=report_dict.get('font', 's'), data=1, width=report_dict['width'],csv_schema=csv_headings)
		report_dict['report_type'] = 'node_res_summary'	#ofcourse you need to reset to node_summary right. ???
		report_dict['topn'] = report_dict['topn_backup']
		report_dict['group_type'] = report_dict['group_type_backup']
		report_dict['statids'] = report_dict.get('statids_backup')
		group_report = XMLSpan('Data Not Available', classId='maptext')
		#If in case it comes from the overview.
		if not sub_report_data_dict and report_dict.get('overview'):
			group_report = []
		self.updateGroupInfo(report_dict)
		return group_report

	def makeUptimeDistribution(self, report_dict):
		report_dict['uptime_distribution'] = 1
		uptimeObjs = self.makeDownTimeReport(report_dict)
		report_dict['uptime_distribution'] = 0
		headings = [('percent', 'Percentage Distribution', '30%', 'left'),('count', 'Count', '20%', 'left')]
		csv_headings = [('percent', 'Percentage Distribution', 30), ('count', 'Count', 20)]

		try:
			slots = maxInputStats.UPTIME_DISTRIBUTION_SLOTS
		except:
			slots = [
				('100 %',(100, 100)),
				('99 %',(99.99, 99)),
				('98 %',(98.99, 98)),
				('97 %',(97.99, 97)),
				('96 %',(96.99, 96)),
				('95 %',(95.99, 95)),
				('90 %',(94.99, 90)),
			]

		def getSlot(uptime_percent):
			for label, slot in slots:
				# if uptime_percent >= 100
				if slot[0] >= uptime_percent >= slot[1]:
					return label
			return 'Others'
				
		temp_dict = {}
		for obj in uptimeObjs:
			uptime_percent = obj.get('uptimepercentage')
			label = getSlot(uptime_percent)
			if not temp_dict.has_key(label):
				temp_dict[label] = 0
			temp_dict[label] = temp_dict[label] + 1
		percent_objs = []
		for label, slot in slots:
			percent_objs.append({'percent':label, 'count':temp_dict.get(label, '-')})			
		if temp_dict.get('Others'):
			percent_objs.append({'percent':'Others', 'count':temp_dict.get('Others')})
		width = report_dict.get('width', 420)
		return XMLReportDataAndSummaryTable(headings, percent_objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)


	def makeDeviceDownTimeSummaryReport(self, report_dict):
		objs = []
		try:
			starttime = report_dict.get('st')
			endtime = report_dict.get('et')
			path = report_dict.get('loc_path')
			path_qry = report_dict.get('res_query').replace("'","*#*#*")
			everest_path = os.getcwd()
			sql_stmt = "select * from generate_downtime_report(%s, %s, '%s','%s')" %(starttime, endtime, str(path_qry),str(everest_path))
			objs = DatabaseServer.executeSQLStatement(sql_stmt)
		except Exception as e:
			raise e
			objs = []
		
		headings = [	('resourcename', 'Path', '18%', 'left'), 
						('bhtotaltime', 'BH Total Duration', '12%', 'left'), 
						('bhuptime', 'BH Uptime', '12%', 'left'),
						('bhdowntime', 'BH Downtime', '12%', 'left'),
						('bhdownper', 'BH Downtime %', '10%', 'left'),
						('bhupper', 'BH Uptime %', '10%', 'left'),
						('nbhtotaltime', 'NBH Total Duration', '12%', 'left'), 
						('nbhuptime', 'NBH Uptime', '12%', 'left'),
						('nbhdowntime', 'NBH Downtime', '12%', 'left'),
						('nbhdownper', 'NBH Downtime %', '10%', 'left'),
						('nbhupper', 'NBH Uptime %', '10%', 'left'),
						('mttr', 'MTTR', '10%', 'left'),
						]
		csv_headings = []
		width = report_dict.get('width', 420)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)


	def makeDownTimeReport(self, report_dict):
		# if safe_int(report_dict['business_hr']) == 1:
		#	return self.makeBusinessDownTimeReport(report_dict)
		logMsg(4, "Inside makeDownTimeReport ***********************", self.module_name)
		self.updateGroupInfo(report_dict)
		headings = [('sno', 'S.No', '5%', 'left'), ('resource_name', 'Resource Name', '20%', 'left'),('portal_display_name', 'Portal Display Name', '20%', 'left')]
		csv_headings = [('sno', 'S.No', 25), ('resource_name', 'Resource Name', 75),('portal_display_name', 'Portal Display Name', 75)]
		# Selected Statistics
		
		#stat_headings, stat_csv_headings = self.getHeadingsForStatistics(report_dict)
		#headings += stat_headings
		#csv_headings += stat_csv_headings
		
		headings += [	('total_duration', 'Total Duration', '20%','left'),
				('unknowntime', 'Maintenance Period', '20%', 'left'),
				('total_duration_without_unknown', 'Total Duration Excluding Maintenance Period', '20%', 'left'),
				('downtime', 'Downtime Duration', '20%', 'left'),
				('uptime', 'Uptime Duration', '20%', 'left'),
				('downtime_percentage', 'Downtime %', '20%','left'),
				('uptime_percentage', 'Uptime %', '20%','left'),
			]
		csv_headings += [   ('total_duration', 'Total Duration', 25),
				    ('unknowntime', 'Maintenance Period', 22),
				    ('total_duration_without_unknown', 'Total Duration Excluding Maintenance Period', 25),
				    ('downtime', 'Downtime Duration', 25),
				    ('uptime', 'Uptime Duration', 22),
				    ('downtime_percentage', 'Downtime %', 22),
				    ('uptime_percentage', 'Uptime %', 22),
				]
		objs = []
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		# Get the statistics values
		#logMsg(4, "Call getDownTimeStatObjs *************************************", self.module_name)
		#report_dict['stat_values'] = self.getDownTimeStatObjs(report_dict, aggregation=report_dict.get('aggregation', 'sum'))
		#logMsg(4, "Call Completed getDownTimeStatObjs *************************************", self.module_name)
		#resids = report_dict.get('data_resids', [])
		# do get downtime objs
		
		#report_dict['downtime_objs'] = self.getDowntimeObjs(report_dict)
		logMsg(4, "Call getDowntimeObjs *************************************", self.module_name)
		report_dict['downtime_objs'],report_dict['uniqOrderList'] = self.getDowntimeObjs(report_dict)
		logMsg(4, "Call Completed getDowntimeObjs *************************************%s %s"%(len(report_dict['downtime_objs']), report_dict['downtime_objs']), self.module_name)
		
		# do downtime calculations
		logMsg(4, "Call doDowntimeCalculations *************************************", self.module_name)
		report_dict['all_outage_by_resid'] = self.doDowntimeCalculations(report_dict)
		logMsg(4, "Call Completed doDowntimeCalculations *************************************", self.module_name)
		objs = self.getDowntimeMonthlyAggregatedData(report_dict)
		logMsg(4, "Call Completed getDowntimeMonthlyAggregatedData *************************************", self.module_name)
		if report_dict.get('uptime_distribution'):
			return objs
		width = report_dict.get('width', 420)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY:
			report_dict['no_resource'] = len(objs)
		logMsg(4, "Completed makeDownTimeReport *************************************", self.module_name)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings, footer="*Availability calculation excludes maintenance period")

	'''
	def makeBusinessDownTimeReport(self, report_dict):
		self.updateGroupInfo(report_dict)
		#print "report_dict['business_hr'] ???? >>> " , report_dict['business_hr']
		headings = [('sno', 'S.No', '5%', 'left'), ('resource_name', 'Resource Name', '20%', 'left'),]
		csv_headings = [('sno', 'S.No', 25), ('resource_name', 'Resource Name', 75),]
		# Selected Statistics
		
		#stat_headings, stat_csv_headings = self.getHeadingsForStatistics(report_dict)
		#headings += stat_headings
		#csv_headings += stat_csv_headings
		
		headings += [	('total_duration', 'Total Duration', '20%','left'),
				('downtime', 'Downtime Duration', '20%', 'left'),
				('uptime', 'Uptime', '20%', 'left'),
				('unknowntime', 'Unknown Time', '20%', 'left'),
				('downtime_percentage', 'Downtime %', '20%','left'),
				('uptime_percentage', 'Uptime %', '20%','left'),
			]
		csv_headings += [   ('total_duration', 'Total Duration', 25),
				    ('downtime', 'Downtime Duration', 25),
				    ('uptime', 'Uptime', 22),
				    ('unknowntime', 'Unknown Time', 22),
				    ('downtime_percentage', 'Downtime %', 22),
				    ('uptime_percentage', 'Uptime %', 22),
				]
		objs = []
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		# Get the statistics values
		report_dict['stat_values'] = self.getDownTimeStatObjs(report_dict, aggregation=report_dict.get('aggregation', 'sum'))
		resids = report_dict.get('data_resids', [])
		# do get downtime objs
		
		# khogendro - modified
		#report_dict['downtime_objs'] = self.getDowntimeObjs(report_dict)
		report_dict['downtime_objs'],report_dict['uniqOrderList'] = self.getBusinessDowntimeObjs(report_dict)
		# khogendro -
		
		# do downtime calculations
		report_dict['all_outage_by_resid'] = self.doBusinessDowntimeCalculations(report_dict)
		report_dict['show_only_hourly_data'] = True
		objs = self.getDowntimeMonthlyAggregatedData(report_dict)
		if report_dict.get('uptime_distribution'):
			return objs
		width = report_dict.get('width', 420)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY:
			report_dict['no_resource'] = len(objs)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)

	def getBusinessDowntimeObjs(self, report_dict):
		start_time = report_dict['st']
		end_time = report_dict['et']		
		resolution = report_dict.get('resolution')
		high_resolution = report_dict.get('high_resolution')
		res_query = report_dict.get('res_query','')
		tables, high_resolution_tables = self.getTables(start_time, end_time, resolution, high_resolution)
		final_objs = {}	
		uniqOrderList = []
		parentId_listResId_dict = {}
		#downtime_statid = self.rawdn_id_map.get('businessdowntime')
		#unknowntime_statid = self.rawdn_id_map.get('businessunknowntime')
		downtime_statid = self.rawdn_id_map.get('downtime')
		unknowntime_statid = self.rawdn_id_map.get('unknowntime')
		for table in tables:
			sql_stmt = 'SELECT s.resid, avg, statid, timestamp as time_stamp,  r.creationTime as "creationTime" FROM %s s, (%s) r WHERE r.resid = s.resid AND s.statid in (%d,%d) AND timeStamp BETWEEN %d AND %d ' %(table, res_query, downtime_statid,unknowntime_statid, start_time, end_time)
			logMsg(4,'getBussDowntimeObjs  -- - %s' %(sql_stmt), self.module_name)
			objs = DatabaseServer.executeSQLStatement(sql_stmt)
			objs = self.checkForBusinessHours(report_dict, objs)
			temp_dict = {}
			temp_objs= []
			for obj in objs:
				key = (obj.get('resid'), obj.get('statid'), obj.get('creationTime'))
				if not temp_dict.has_key(key):
					temp_dict[key] = 0
				temp_dict[key] += safe_int(obj.get('avg'))
			for key, avg in	temp_dict.iteritems():
				d = dict(zip(['resid', 'statid', 'creationTime', 'avg'], list(key) + [avg]))
				temp_objs.append(d)
			objs = temp_objs						

			classify_objs = classifyObjs(objs, lambda a: a.get('resid', ''))
			
			if classify_objs and classify_objs != -1:
				for resid,classify_obj in classify_objs.items():
					if final_objs.has_key(resid):# value exists
						for item in classify_obj:
							if item.get('statid') == downtime_statid:
								final_objs[resid]['downtime_statid'] = final_objs.get(resid).get('downtime_statid',0)+item.get('avg')
							elif item.get('statid') == unknowntime_statid:
								final_objs[resid]['unknowntime_statid'] = final_objs.get(resid).get('unknowntime_statid',0)+item.get('avg')
							else:
								final_objs[resid][item.get('statid')] = item.get('avg')
					else:
						#Value does not exists
						temp = {}
						for item in classify_obj:
							if item.get('statid') == downtime_statid:
								temp['downtime_statid'] = item.get('avg',0)
							elif item.get('statid') == unknowntime_statid:
								temp['unknowntime_statid'] = item.get('avg',0)
							else:
								temp[item.get('statid')] = item.get('avg',0)
							parentId = item.get('parentid','')
							# res_CreationTime = safe_int(item.get('creationTime'))
							res_CreationTime = safe_int(timeStrTotimeInt(item.get('creationTime')))
						temp['creationTime'] = res_CreationTime
						final_objs[resid] = temp#obj.get('avg',0)
						temp_list = parentId_listResId_dict.get(parentId,[])
						temp_list.append(resid)											
						parentId_listResId_dict[parentId] = temp_list		
		for obj in parentId_listResId_dict:
			uniqOrderList = uniqOrderList + parentId_listResId_dict.get(obj,[])
		return [final_objs],uniqOrderList
	'''
	
	def getHourBaseTimeBetweenTimes(self, st, et):
		try:
			if st > et:
				return {}
			hour_times = {}
			tl = time.localtime(st)
			sth = time.mktime((tl[0], tl[1], tl[2], tl[3], 0, 0, 0, 0, tl[8]))
			hour_times[(safe_int(sth))] = 1
			
			tl = time.localtime(et)
			eth = time.mktime((tl[0], tl[1], tl[2], tl[3], 0, 0, 0, 0, tl[8]))
			if safe_int(eth) not in hour_times:
				hour_times[(safe_int(eth))] = 1
			
			sth += 3600 #Increment by hour
			while sth < eth:
				hour_times[(safe_int(sth))] = 1
				sth += 3600 #Increment by hour
			return hour_times
		except Exception, msg:
			logExceptionMsg(4, "Exception in getHourBaseTimeBetweenTimes - %s - %s - %s"%(st, et, msg), self.module_name)
			return {}

	def updateUnknownObjs(self, objs, start_time, end_time, tables, table, downtime_statid, unknowntime_statid):
		if objs and table.lower().find('tblhour') != -1 and maxInputStats.ADD_UNKNOWN_TIME_FOR_BUSINESS_HOUR:
			logMsg(4,'Going to add unknown times - %s, %s'%(tables, len(objs)), self.module_name)
			try:
				#Get all Hours
				hour_times = self.getHourBaseTimeBetweenTimes(start_time, end_time)
				logMsg(4,'number of hour_times - %s' %len(hour_times), self.module_name)
				#print "hour_times ",hour_times
				if hour_times:
					r_t_s = {}
					def update_memory(a):
						if not r_t_s.has_key(a['resid']):
							r_t_s[a['resid']] = {'creationTime': a['creationTime']}
						
						if not r_t_s[a['resid']].has_key('time_stamp'):
							r_t_s[a['resid']]['time_stamp'] = {a['time_stamp'] : 1}
						else:
							r_t_s[a['resid']]['time_stamp'][a['time_stamp']] = 1
					map(lambda a: update_memory(a), objs)
					#print "r_t_s ", r_t_s
					logMsg(4,'Done memory Update - %s' %len(r_t_s), self.module_name)

					def check_time(a, resid, times_dict):
						try:
							if a >= safe_int(times_dict.get('creationTime').strftime('%s')) and (not times_dict.get('time_stamp', {}).has_key(a)):
								objs.append({'resid': resid, 'time_stamp': a, 'statid': downtime_statid, 'avg' : 0, 'min':0, 'max':0, 'count': 1, 'creationTime': times_dict.get('creationTime')})
								objs.append({'resid': resid, 'time_stamp': a, 'statid': unknowntime_statid, 'avg' : 3600, 'min':3600, 'max':3600, 'count': 1, 'creationTime': times_dict.get('creationTime')})
								#print {'resid': resid, 'time_stamp': a, 'statid': unknowntime_statid, 'avg' : 3600, 'min':3600, 'max':3600, 'count': 1}
						except:
							if a >= times_dict.get('creationTime') and (not times_dict.get('time_stamp', {}).has_key(a)):
								objs.append({'resid': resid, 'time_stamp': a, 'statid': downtime_statid, 'avg' : 0, 'min':0, 'max':0, 'count': 1, 'creationTime': times_dict.get('creationTime')})
								objs.append({'resid': resid, 'time_stamp': a, 'statid': unknowntime_statid, 'avg' : 3600, 'min':3600, 'max':3600, 'count': 1, 'creationTime': times_dict.get('creationTime')})
								#print {'resid': resid, 'time_stamp': a, 'statid': unknowntime_statid, 'avg' : 3600, 'min':3600, 'max':3600, 'count': 1}
					map(lambda z : map(lambda a: check_time(a, z[0], z[1]), hour_times.keys()), r_t_s.items())
			except Exception, msg:
				logExceptionMsg(4, "Exception in filling unknown time  %s"%(msg), self.module_name)
			logMsg(4,'Unknown times added completed- %s'%(len(objs)), self.module_name)
		#print "objs", objs
		return objs

	def getDowntimeObjs(self,report_dict):
		start_time = report_dict['st']
		end_time = report_dict['et']
		if safe_int(end_time) > safe_int(time.time()):
			end_time = safe_int(time.time())
		resolution = report_dict.get('resolution')
		high_resolution = report_dict.get('high_resolution')
		res_query = report_dict.get('res_query','')
		tables, high_resolution_tables = self.getTables(start_time, end_time, resolution, high_resolution)
		final_objs = {}	
		uniqOrderList = []
		parentId_listResId_dict = {}
		downtime_statid = self.rawdn_id_map.get('downtime')
		unknowntime_statid = self.rawdn_id_map.get('unknowntime')
		objs = []
		table = ''
		for table in tables:
			if safe_int(report_dict['business_hr']) == 1 or table.lower().find('tblhour') != -1:
				sql_stmt = 'SELECT s.resid, avg, statid, timestamp as time_stamp, r.creationTime as "creationTime" FROM %s s, (%s) r WHERE r.resid = s.resid AND s.statid in (%d,%d) AND timeStamp BETWEEN %d AND %d ' %(table, res_query, downtime_statid,unknowntime_statid, start_time, end_time)
			else:
				sql_stmt = 'SELECT s.resid, SUM(s.avg) avg, statid,r.creationTime as "creationTime" FROM %s s, (%s) r WHERE r.resid = s.resid AND s.statid in (%d,%d) AND timeStamp BETWEEN %d AND %d GROUP BY s.resid,statid,r.creationTime' %(table, res_query, downtime_statid, unknowntime_statid, start_time, end_time)
			logMsg(4,'getDowntimeObjs - %s' %(sql_stmt), self.module_name)
			sobjs = DatabaseServer.executeSQLStatement(sql_stmt)
			#print "\n\n"
			#print "objs- DB", objs
			#print "\n\n"
			if type(sobjs) != type([]):
				sobjs = []
			for sobj in sobjs:
				if sobj['statid'] == unknowntime_statid:
					sobj['avg'] = 0
			objs += sobjs 

		logMsg(4,'getDowntimeObjs from Database - %s' %(len(objs)), self.module_name)
		#Added For Filling Unknown Time
		objs = self.updateUnknownObjs(objs, start_time, end_time, tables, table, downtime_statid, unknowntime_statid)
		logMsg(4,'getDowntimeObjs after unknown added - %s' %(len(objs)), self.module_name)

		if safe_int(report_dict['business_hr']) == 1:
			logMsg(4,'It is bussiness hr  - %s' %(len(objs)), self.module_name)
			profile_dn = report_dict.get("business_hr_profile",'')
			if profile_dn:
				logMsg(4,'Before checkForBusinessHours - %s' %(len(objs)), self.module_name)
				objs = self.checkForBusinessHours(report_dict, objs)
				logMsg(4,'After checkForBusinessHours - %s' %(len(objs)), self.module_name)
		temp_dict = {}
		temp_objs= []
		for obj in objs:
			key = (obj.get('resid'), obj.get('statid'), obj.get('creationTime'))
			if not temp_dict.has_key(key):
				temp_dict[key] = 0
			temp_dict[key] += safe_int(obj.get('avg'))
		for key, avg in	temp_dict.iteritems():
			d = dict(zip(['resid', 'statid', 'creationTime', 'avg'], list(key) + [avg]))
			temp_objs.append(d)
		objs = temp_objs
		#print "\n\n"
		#print "objs- MAdded", objs
		#print "\n\n"
		classify_objs = classifyObjs(objs, lambda a: a.get('resid', ''))
		print "classify_objs",classify_objs
		if classify_objs and classify_objs != -1:
			for resid,classify_obj in classify_objs.items():
				print "resid", resid
				print "classify_obj",classify_obj
				if final_objs.has_key(resid):#value exists
					print "\n\n Inside IFFFFFFFFFFFFFFF"
					for item in classify_obj:
						if item.get('statid') == downtime_statid:
							final_objs[resid]['downtime_statid'] = final_objs.get(resid).get('downtime_statid',0)+item.get('avg')
						elif item.get('statid') == unknowntime_statid:
							final_objs[resid]['unknowntime_statid'] = final_objs.get(resid).get('unknowntime_statid',0)+item.get('avg')
						else:
							final_objs[resid][item.get('statid')] = item.get('avg')
				else:
					#Value does not exists
					temp = {}
					for item in classify_obj:
						if item.get('statid') == downtime_statid:
							temp['downtime_statid'] = item.get('avg',0)
						elif item.get('statid') == unknowntime_statid:
							temp['unknowntime_statid'] = item.get('avg',0)
						else:
							temp[item.get('statid')] = item.get('avg',0)
						parentId = item.get('parentid','')
						#res_CreationTime = safe_int(timeStrTotimeInt(item.get('creationTime')))
						res_CreationTime = item.get('creationTime')
					temp['creationTime'] = res_CreationTime
					final_objs[resid] = temp#obj.get('avg',0)
					temp_list = parentId_listResId_dict.get(parentId,[])
					temp_list.append(resid)
					parentId_listResId_dict[parentId] = temp_list
		for obj in parentId_listResId_dict:
			uniqOrderList = uniqOrderList + parentId_listResId_dict.get(obj,[])
		return [final_objs],uniqOrderList

	def getHeadingsForStatistics(self, report_dict):
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		unit_map = {-1: '', 0:'', 1:'K', 2:'M', 3:'G', 4:'T'}
		unit_division_factor_map = {'bps': 1000, 'Bytes': 1024, '*bytes': 1024, 'bytes': 1024, '**bytes': 1024}
		aggregation = 'avg'
		headings = []
		csv_headings = []
		vsat_affected_calc = 0
		for stat in stats:
			# For Backhaul Daily Report - No.of VSATs Affected column
			if stat == safe_int(self.statname_id_map.get('No of VSATs Up')):
				vsat_affected_calc = 1
				continue
			unit = self.statid_unit_map.get(stat)
			if stat_unit != None and stat_unit != 'None' and stat_unit != -1:
				if unit in self.units_to_be_formatted:
					if unit.startswith('**'):
						unit = unit[2:]
					elif unit.startswith('*'):
						unit = unit[1:]
					if unit == 'bytes':
						aggregation = 'sum'
					unit = unit_map.get(safe_int(stat_unit)) + unit
			stat_name = self.statid_name_map.get(stat)
			headings.append((stat, "%s (%s)"%(stat_name, unit), '10%', 'left'))
			csv_headings.append((stat, '%s (%s)'%(stat_name, unit), 20))
		report_dict['aggregation'] = aggregation
		report_dict['vsat_affected_calc'] = vsat_affected_calc
		return headings, csv_headings

	'''
	def getHeadingsForStatistics(self, report_dict):
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		unit_division_factor_map = {'bps': 1000, 'Bytes': 1024, '*bytes': 1024, 'bytes': 1024, '**bytes': 1024}
		aggregation = 'avg'
		headings = []
		csv_headings = []
		for stat in stats:
			if stat == self.rawdn_id_map.get('downtime'):
				continue
			unit = self.statid_unit_map.get(stat)
			if stat_unit != None and stat_unit != 'None' and stat_unit != -1:
				if unit in self.units_to_be_formatted:
					if unit.startswith('**'):
						unit = unit[2:]
					elif unit.startswith('*'):
						unit = unit[1:]
					if unit == 'bytes':
						aggregation = 'sum'
			stat_name = self.statid_name_map.get(stat)
			headings.append((stat, "%s (%s)"%(stat_name, unit), '10%', 'left'))
			csv_headings.append((stat, '%s (%s)'%(stat_name, unit), 20))
		report_dict['aggregation'] = aggregation
		return headings, csv_headings
	'''	
	def getDownTimeStatObjs(self, report_dict, aggregation='sum'):
		if aggregation == 'sum':
			formula = " SUM(s.avg) avg "
		else:
			formula = " AVG(s.avg) avg "
		return self.getStatObjs(report_dict, formula, aggregation=aggregation)
		
	def getStatObjs(self, report_dict, formula, aggregation='sum'):
		#This method will return all the selected statistics value for the selected resids.
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		res_query = report_dict.get('res_query')
		resolution = report_dict.get('resolution')
		high_resolution = report_dict.get('high_resolution')
		tables, high_resolution_tables = self.getTables(start_time, end_time, resolution, high_resolution)
		all_objs = []
		for table in tables:
			# khogendro - modified for the order of the list of down devices
			sql_stmt = "SELECT s.resid, s.statid, %s FROM %s s, (%s) r WHERE r.resid = s.resid AND s.statid IN (%s) AND timeStamp BETWEEN %d AND %d GROUP BY s.resid, s.statid" %(formula, table, res_query, str(stats)[1:-1], start_time, end_time)
			#sql_stmt = "SELECT r.parentid,s.resid, s.statid, %s FROM %s s, (%s) r WHERE r.resid = s.resid AND s.statid IN (%s) AND timeStamp BETWEEN %d AND %d GROUP BY s.resid, s.statid,r.parentid order by r.parentid" %(formula, table, res_query, str(stats)[1:-1], start_time, end_time)
			# khogendro - modified
			
			logMsg(2, 'getStatObjs -- %s' %sql_stmt, self.module_name)
			objs = DatabaseServer.executeSQLStatement(sql_stmt)
			if objs and objs != -1:
				all_objs += objs
		resid_statid_map = {}
		for obj in all_objs:
			resid = safe_int(obj.get('resid'))
			statid = safe_int(obj.get('statid'))
			resid_statid_map[(resid, statid)] = safe_float(resid_statid_map.get((resid, statid))) + safe_float(obj.get('avg'))
		report_dict['data_resids'] = unique(map(lambda a: safe_int(a.get('resid')), all_objs))
		logMsg(2, 'Data resids - %s' %(len(report_dict['data_resids'])), self.module_name)
		return resid_statid_map

	# def doBusinessDowntimeCalculations(self, report_dict):
		# top_value = safe_int(report_dict.get('topn'))
		# downtime_calc_objs = []
		# all_outage_by_resid = {}
		# total_timeduration = 0
		# start_time = report_dict.get('st')
		# end_time = report_dict.get('et')
		# #If end time is greater than current time change the end time to curr time. since we dont have data for future stats.
		# if safe_int(end_time) > safe_int(time.time()):
			 # end_time = time.time()
		# total_duration = safe_int(end_time)+1 - safe_int(start_time)
		# profile_dn = report_dict.get("business_hr_profile",'')
		# if not profile_dn:
			# bus_objs = self.businessHrServer.getBusObjs()
		# else:
			# bus_objs = self.businessHrServer.getBusObjs(profile_dn)
			# if not bus_objs:
				  # bus_objs = self.businessHrServer.getBusObjs()
		# business_hr_map = dict([(safe_int(bus.get('dn').split('_')[-1]), bus.get('time').split('-')) for bus in bus_objs])
		# total_duration = getTotalDurationForBusinessHr(start_time, safe_int(end_time), business_hr_map)

		# downtime_alerts = report_dict.get('downtime_alerts', {})
		# resids = report_dict.get('data_resids', [])
		# downtime_objs = report_dict.get('downtime_objs', [])
		# #downtime_objs = [resid: {'unknowntime_statid': avg, 'downtime_statid': avg},resid: {'unknowntime_statid': avg, 'downtime_statid': avg}]
		# if downtime_objs != []:
			# downtime_objs = downtime_objs[0]
		# # khogendro - modified for the order of the list of down devices
		# uniqOrderList = report_dict.get('uniqOrderList','')
		# # khogendro - end here
		# for resid,avg_values in downtime_objs.items():
			# #calc total downtime %
			# #downtime = safe_int(downtime_objs.get(resid))
			# #uptime = safe_int(total_duration - downtime)
			# #if total_duration > 0 and downtime < total_duration:
			# #	downtimepercentage = round((safe_float(downtime)/total_duration)*100,2)
			# #else:
			# #	downtimepercentage = 0
			# downtime = safe_int(avg_values.get('downtime_statid'))
			
			# unknowntime = safe_int(avg_values.get('unknowntime_statid'))
			# creationTime = avg_values.get('creationTime')
			# if type(creationTime) == datetime.datetime:
				# creationTime = safe_int(creationTime.strftime('%s'))
			# if safe_int(start_time) < creationTime:
				# temp_start_time = creationTime
				# #try:
				# #	x = creationTime
				# #	time_tup = time.localtime(x)
				# #	secToRemove = time_tup[4]*60+time_tup[5]
				# #	temp_start_time = x-secToRemove	
				# #except:
				# #	pass
				# #temp_total_duration = safe_int(end_time)+1 - safe_int(temp_start_time)
				# temp_total_duration = getTotalDurationForBusinessHr(temp_start_time, safe_int(end_time)+1, business_hr_map)
				# #if safe_int(report_dict.get('timescale')) == 2:#Since if last 60 mins is selected no need to add 1sec
				# #	temp_total_duration = safe_int(end_time) - safe_int(temp_start_time) zzzzzzzzz

				# uptime = safe_int(temp_total_duration - downtime - unknowntime)
				# if temp_total_duration > 0 and downtime < temp_total_duration and downtime >= 0 :
					# downtimepercentage = round((safe_float(downtime)/temp_total_duration )*100,2)
				# else:
					# downtimepercentage = 100
				# calculatedObj = {
						# 'total_duration':temp_total_duration,
						# 'unknowntime':unknowntime,
						# 'downtime':downtime,
						# 'uptime':uptime,
						# 'downtime_percentage':str(downtimepercentage)+'%',
						# 'downtimepercentage': downtimepercentage,
						# 'uptimepercentage': 100.0 - downtimepercentage,
						# 'uptime_percentage': str(100.0 - downtimepercentage)+'%',
					# }
			# else:
				# uptime = safe_int(total_duration - downtime - unknowntime)
				# if total_duration > 0 and downtime < total_duration and downtime >= 0:
					# downtimepercentage = round((safe_float(downtime)/total_duration)*100,2)
				# else:
					# downtimepercentage = 100
				# calculatedObj = {
						# 'total_duration':total_duration,
						# 'unknowntime':unknowntime,
						# 'downtime':downtime,
						# 'uptime':uptime,
						# 'downtime_percentage':str(downtimepercentage)+'%',
						# 'downtimepercentage': downtimepercentage,
						# 'uptimepercentage': 100.0 - downtimepercentage,
						# 'uptime_percentage': str(100.0 - downtimepercentage)+'%',
					# }
			# # khogendro - modified for the order of the list of down devices
			# #downtime_calc_objs.append((downtime, (resid, calculatedObj)))
			# position = uniqOrderList.index(resid)
			# uniqOrderList.remove(resid)
			# uniqOrderList.insert(position,(downtime, (resid, calculatedObj)))
		# downtime_calc_objs = uniqOrderList
		# # khogendro - end here.
		# # get the top value based on total downtime
		# if top_value and len(downtime_calc_objs) > top_value:
			# downtime_calc_objs = downtime_calc_objs[:top_value]
		# return map(lambda a: a[1], downtime_calc_objs)		

	def doDowntimeCalculations(self, report_dict):
		top_value = safe_int(report_dict.get('topn'))
		downtime_calc_objs = []
		all_outage_by_resid = {}
		total_timeduration = 0
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		'''
		#To calculate total duration-- send 1 as the last parameter in timeBetween().
		total_timeduration = timeBetween(start_time, end_time,report_dict.get('timebetween',''),1)
		if report_dict.get('timebetween','') != '' and total_timeduration != 0:
			total_duration = safe_int(total_timeduration)
		else:
			total_duration = safe_int(end_time) - safe_int(start_time)
		'''
		#If end time is greater than current time change the end time to curr time. since we dont have data for future stats.
		if safe_int(end_time) > safe_int(time.time()):
			 end_time = time.time()
		# total_duration = safe_int(end_time)+1 - safe_int(start_time)
		if safe_int(report_dict['business_hr']) == 1:
			profile_dn = report_dict.get("business_hr_profile",'')
			if not profile_dn:
				total_duration = safe_int(end_time)+1 - safe_int(start_time)
			else:
				bus_objs = self.businessHrServer.getBusObjs(profile_dn)
				if not bus_objs:
					bus_objs = self.businessHrServer.getBusObjs()
				business_hr_map = dict([(safe_int(bus.get('dn').split('_')[-1]), bus.get('time').split('-')) for bus in bus_objs])
				total_duration = getTotalDurationForBusinessHr(start_time, safe_int(end_time)+1, business_hr_map)
		else:
			total_duration = safe_int(end_time)+1 - safe_int(start_time)
		if safe_int(report_dict.get('timescale')) == 2:#Since if last 60 mins is selected no need to add 1sec
			total_duration = safe_int(end_time) - safe_int(start_time)
		downtime_alerts = report_dict.get('downtime_alerts', {})
		resids = report_dict.get('data_resids', [])
		downtime_objs = report_dict.get('downtime_objs', [])
		#downtime_objs = [resid: {'unknowntime_statid': avg, 'downtime_statid': avg},resid: {'unknowntime_statid': avg, 'downtime_statid': avg}]
		if downtime_objs != []:
			downtime_objs = downtime_objs[0]
		# khogendro - modified for the order of the list of down devices
		uniqOrderList = report_dict.get('uniqOrderList','')
		# khogendro - end here
		logMsg(4, "Inside total_duration %s"%total_duration, self.module_name)

		for resid,avg_values in downtime_objs.items():
			#calc total downtime %
			#downtime = safe_int(downtime_objs.get(resid))
			#uptime = safe_int(total_duration - downtime)
			#if total_duration > 0 and downtime < total_duration:
			#	downtimepercentage = round((safe_float(downtime)/total_duration)*100,2)
			#else:
			#	downtimepercentage = 0
			downtime = safe_int(avg_values.get('downtime_statid'))
			unknowntime = safe_int(avg_values.get('unknowntime_statid'))
			creationTime = avg_values.get('creationTime')
			logMsg(4, "Inside downtime=%s, unknowntime=%s, creationTime=%s "%(downtime, unknowntime, creationTime), self.module_name)
			if type(creationTime) == datetime.datetime:
				creationTime = safe_int(creationTime.strftime('%s'))
			if safe_int(start_time) < creationTime:
				logMsg(4, "Inside lesser creation time %s"%creationTime, self.module_name)
				temp_start_time = creationTime
				temp_total_duration = safe_int(end_time)+1 - safe_int(temp_start_time)
				if safe_int(report_dict.get('timescale')) == 2:#Since if last 60 mins is selected no need to add 1sec
					temp_total_duration = safe_int(end_time) - safe_int(temp_start_time)
				
				#Added for Adjusting
				if safe_int(downtime) > safe_int(temp_total_duration):
					downtime = temp_total_duration
				if safe_int(downtime) + safe_int(unknowntime) > safe_int(temp_total_duration):
					unknowntime = safe_int(temp_total_duration) - safe_int(downtime)
				#Added for Adjusting
				
				#Remove the Unknown from Total Time
				temp_total_duration_without_unknown = temp_total_duration - unknowntime
				#Remove the Unknown from Total Time
				
				# uptime = safe_int(temp_total_duration - downtime - unknowntime)
				uptime = safe_int(temp_total_duration_without_unknown - downtime)
				if temp_total_duration_without_unknown > 0 and downtime >= 0 :
					downtimepercentage = round((safe_float(downtime)/temp_total_duration_without_unknown )*100,2)
				else:
					downtimepercentage = 0
				
				if temp_total_duration_without_unknown > 0 and uptime >= 0 :
					upimepercentage = round((safe_float(uptime)/temp_total_duration_without_unknown )*100,2)
				else:
					upimepercentage = 0
				calculatedObj = {
						'total_duration':temp_total_duration,
						'total_duration_without_unknown':temp_total_duration_without_unknown,
						'unknowntime':unknowntime,
						'downtime':downtime,
						'uptime':uptime,
						'downtime_percentage':str(downtimepercentage)+'%',
						'downtimepercentage': downtimepercentage,
						'uptimepercentage': upimepercentage,
						'uptime_percentage': str(upimepercentage)+'%',
					}
			else:
				#Added for Adjusting
				logMsg(4, "Inside Else lesser creation time %s"%creationTime, self.module_name)
				if safe_int(downtime) > safe_int(total_duration):
					downtime = total_duration
					logMsg(4, "Adjusted downtime %s"%downtime, self.module_name)
				if safe_int(downtime) + safe_int(unknowntime) > safe_int(total_duration):
					unknowntime = safe_int(total_duration) - safe_int(downtime)
					logMsg(4, "Adjusted unknowntime %s"%unknowntime, self.module_name)
				#Added for Adjusting
				logMsg(4, "Inside downtime=%s, unknowntime=%s lesser creation time %s"%(downtime, unknowntime, creationTime), self.module_name)
				total_duration_without_unknown = total_duration - unknowntime
				uptime = safe_int(total_duration_without_unknown - downtime)
				if total_duration_without_unknown > 0 and downtime >= 0:
					downtimepercentage = round((safe_float(downtime)/total_duration_without_unknown)*100,2)
				else:
					downtimepercentage = 0
				if total_duration_without_unknown > 0 and uptime >= 0 :
					upimepercentage = round((safe_float(uptime)/total_duration_without_unknown )*100,2)
				else:
					upimepercentage = 0

				calculatedObj = {
						'total_duration':total_duration,
						'total_duration_without_unknown':total_duration_without_unknown,
						'unknowntime':unknowntime,
						'downtime':downtime,
						'uptime':uptime,
						'downtime_percentage':str(downtimepercentage)+'%',
						'downtimepercentage': downtimepercentage,
						'uptimepercentage': upimepercentage,
						'uptime_percentage': str(upimepercentage)+'%',
					}
			# khogendro - modified for the order of the list of down devices
			#downtime_calc_objs.append((downtime, (resid, calculatedObj)))
			position = uniqOrderList.index(resid)
			uniqOrderList.remove(resid)
			uniqOrderList.insert(position,(downtime, (resid, calculatedObj)))
		downtime_calc_objs = uniqOrderList
		'''
		downtime_calc_objs.sort(sortTuple1)
		downtime_calc_objs.reverse()
		'''
		# khogendro - end here.
		# get the top value based on total downtime
		if top_value and len(downtime_calc_objs) > top_value:
			downtime_calc_objs = downtime_calc_objs[:top_value]
		return map(lambda a: a[1], downtime_calc_objs)		

	def getDowntimeMonthlyAggregatedData(self, report_dict):
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		res_path_map = report_dict.get('res_path_map', {})
		res_portal_displayname_map = report_dict.get('res_portal_displayname_map', {})
		stat_values = report_dict.get('stat_values', {})
		downtime_alerts = report_dict.get('downtime_alerts', {})
		objs = []
		all_outage_by_resid = report_dict.get('all_outage_by_resid', [])
		sno = 1
		for resid, calc_obj in all_outage_by_resid:
			path_name = res_path_map.get(resid, '')
			portal_display_name = res_portal_displayname_map.get(resid, '')
			obj = {'resource_name': path_name.split('/')[-1], 'portal_display_name':portal_display_name}
			obj.update(calc_obj)
			for stat in stats:
				cur_unit =  self.statid_unit_map.get(stat, '')
				obj[stat] = round(stat_values.get((resid, stat),0),2)
			objs.append(obj)
		#To calculate the column wise total
		total_device_down = sum(map(lambda a : safe_int(a.get("device_down")), objs))
		total_downtime = sum(map(lambda a : a.get("downtime"), objs))
		total_uptime = sum(map(lambda a : a.get("uptime"), objs))
		total_total_duration = sum(map(lambda a : a.get("total_duration"), objs))
		total_total_duration_without_unknown = sum(map(lambda a : a.get("total_duration_without_unknown"), objs))
		total_unknowntime = sum(map(lambda a : a.get("unknowntime"), objs))
		#total_downtime_percentage = sum(map(lambda a : a.get("downtime_percentage"), objs))
		try:
			total_downtime_percentage = sum(map(lambda a : safe_float(a.get("downtime_percentage").replace('%','')), objs)) / len(map(lambda a : safe_float(a.get("downtime_percentage").replace('%','')), objs))
		except:
			total_downtime_percentage = 0
		try:
			total_uptime_percentage = sum(map(lambda a : safe_float(a.get("uptime_percentage").replace('%','')), objs)) / len(map(lambda a : safe_float(a.get("uptime_percentage").replace('%','')), objs))
		except:
			total_uptime_percentage = 0
		#total_uptime_percentage = 100 -  total_downtime_percentage
		total_obj = {
				'resource_name': 'Total', 
				'total_duration_without_unknown': str(makeTimeDeltaHour(total_total_duration_without_unknown)), #str(safe_int(safe_float(total_total_duration)/60)) + " min",
				'total_duration': str(makeTimeDeltaHour(total_total_duration)), #str(safe_int(safe_float(total_total_duration)/60)) + " min",
				'downtime': str(makeTimeDeltaHour(total_downtime)), #str(safe_int(safe_float(total_downtime)/60)) + " min",
				'uptime':str(makeTimeDeltaHour(total_uptime)), #str(safe_int(safe_float(total_uptime)/60)) + " min",
				'unknowntime':str(makeTimeDeltaHour(total_unknowntime)), #str(safe_int(safe_float(total_unknowntime)/60)) + " min",
				'downtime_percentage': str(round(total_downtime_percentage,2))+'%',
				'uptime_percentage': str(round(total_uptime_percentage,2))+'%',
				'sno': ' ',
			}
		for stat in stats:
			cur_unit =  self.statid_unit_map.get(stat, '')
			total_obj[stat] = " "
		sno = 1
		objs.sort(lambda a, b: cmp(a.get('downtimepercentage', ''), b.get('downtimepercentage', '')))
		for obj in objs:	
			obj['sno'] = str(sno)
			if report_dict.get('show_only_hourly_data'):
				obj['total_duration'] = str(makeTimeDeltaHour(obj.get('total_duration')))
				obj['total_duration_without_unknown'] = str(makeTimeDeltaHour(obj.get('total_duration_without_unknown')))
				obj['downtime'] = str(makeTimeDeltaHour(obj.get('downtime')))
				obj['uptime'] = str(makeTimeDeltaHour(obj.get('uptime')))	
				obj['unknowntime'] = str(makeTimeDeltaHour(obj.get('unknowntime')))
			else:
				obj['total_duration'] = str(makeTimeDelta(obj.get('total_duration')))
				obj['total_duration_without_unknown'] = str(makeTimeDelta(obj.get('total_duration_without_unknown')))
				obj['downtime'] = str(makeTimeDelta(obj.get('downtime')))
				obj['uptime'] = str(makeTimeDelta(obj.get('uptime')))
				obj['unknowntime'] = str(makeTimeDelta(obj.get('unknowntime')))
			sno += 1
		if maxInputStats.REPORT_DISPLAY_TOTAL and objs:
			objs.append(total_obj)
		return objs

	def doSLAComputation(self, report_dict):
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		resids = report_dict.get('res_path_map', {}).keys()
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		resolution = report_dict.get('resolution')
		time_filter = timeBetween(start_time, end_time, report_dict.get('timebetween',''))
		red_val = safe_float(report_dict.get('report_obj', {}).get('red_val'))
		yellow_val = safe_float(report_dict.get('report_obj', {}).get('yellow_val'))
		# for the positive polarity
		if red_val > yellow_val:
			operator = '<'
		# for the negative polarity
		else:
			operator = '>'
		sla_condn = []
		for statid in stats:
			sla_condn.append(" %s %s %s %s " %(self.statid_name_map.get(statid), operator, red_val, self.statid_unit_map.get(statid)))
		report_dict['condn_str'] = '%s [SLA Condition: %s]'%(XMLSpace(5), string.join(sla_condn, ' AND '))
		tables, high_resolution_tables = self.getTables(start_time, end_time, resolution, resolution)
		temp_table_name = ''
		if tables:
			# create temporary table
			temp_table_name = getUniqueString()			
			sql_stmt = "CREATE TABLE %s (resid INT, statid INT, timeStamp INT, avg INT, comply INT)" %(temp_table_name)
			logMsg(2, 'Create SQL Statement - %s in doSLAComputation' %sql_stmt, self.module_name)
			DatabaseServer.executeSQLStatement(sql_stmt)
			for table in tables:
				# insert the raw polled points data for the selected duration to the temporary table with sla rule applied.
				sql_stmt = "INSERT INTO %s (resid, statid, timeStamp, avg, comply) SELECT resid, statid, timeStamp, avg, CASE WHEN avg %s %s THEN 1 ELSE 0 END as comply FROM %s WHERE statid IN (%s) AND resid IN (%s) %s" %(temp_table_name, operator, red_val, table, str(stats)[1:-1], str(resids)[1:-1], time_filter)
				logMsg(2, 'Select Insert SQL Statement - %s in doSLAComputation' %sql_stmt, self.module_name)
				DatabaseServer.executeSQLStatement(sql_stmt)
		return temp_table_name

	def makeSLASummaryReport(self, report_dict):
		self.updateGroupInfo(report_dict)
		headings = [
						('sno', 'S.No', '5%', 'left'), 
						('_resource_name', 'Resource Name', '30%', 'left'), 
						('_avg', 'Average', '10%', 'left'), 
						('_min', 'Minimum', '10%', 'left'), 
						('_max', 'Maximum', '10%', 'left'), 
						('_sla', 'SLA Complaince', '10%', 'left'),
						('fail_count', 'Breached Count', '10%', 'left'), 
					]
		csv_headings = [
						('sno', 'S.No', 25), ('resource_name', 'Resource Name', 75),
						('_avg', 'Average', 20), ('_min', 'Minimum', '20'), ('_max', 'Maximum', '20'), 
						('sla', 'SLA %', '20'), ('fail_count', 'Breached Count', '20'), 
					]
		objs = []
		data_objs = []
		temp_table_name = self.doSLAComputation(report_dict)
		business_hr = safe_int(report_dict.get('business_hr', ''))
		if temp_table_name:
			if business_hr == 0:
				sql_stmt = "SELECT x.resid, x.statid, AVG(x.avg) avg, MIN(x.avg) min, MAX(x.avg) max, COUNT(x.avg) AS row_count, SUM(x.comply) pass_count, COUNT(x.comply) - SUM(x.comply) as fail_count, AVG(x.comply*100) AS comply_percentage FROM %s x GROUP BY x.resid, x.statid ORDER BY comply_percentage ASC" %temp_table_name
				logMsg(2, 'All Hours SQL Statement - %s in makeSLASummaryReport' %sql_stmt, self.module_name)
				objs = DatabaseServer.executeSQLStatement(sql_stmt)
			# Business or Non-Business Hour Option is selected.
			else:
				sql_stmt = "SELECT x.resid, x.statid, timeStamp, timeStamp as time_stamp, avg, x.comply*100 AS comply_percentage FROM %s x" %temp_table_name
				logMsg(2, 'Business / Non Business Hours SQL Statement - %s in makeSLASummaryReport' %sql_stmt, self.module_name)
				data_objs = DatabaseServer.executeSQLStatement(sql_stmt)
				# filter data for business or non business hours 
				data_objs = self.checkForBusinessHours(report_dict, data_objs)
				# group data based on resid and statid
				res_stat_objs = classifyObjs(data_objs, lambda a: (safe_int(a.get('resid')), safe_int(a.get('statid'))))
				# aggregate data for resid and statid
				for ((resid, statid), res_stat_obj) in res_stat_objs.items():
					obj = {}
					obj['resid'] = resid
					obj['statid'] = statid
					values = map(lambda a: safe_int(a.get('avg')), res_stat_obj)
					sla_values = map(lambda a: safe_int(a.get('comply_percentage')), res_stat_obj)
					obj['min'] = min(values)
					obj['max'] = max(values)
					obj['avg'] = avg(values)
					obj['row_count'] = len(values)
					obj['pass_count'] = len(filter(lambda a: safe_int(a.get('comply_percentage')) == 100, res_stat_obj))
					obj['fail_count'] = len(filter(lambda a: safe_int(a.get('comply_percentage')) == 0, res_stat_obj))
					obj['comply_percentage'] = avg(sla_values)
					objs.append(obj)
				objs.sort(lambda a, b: cmp(a.get('comply_percentage'), b.get('comply_percentage')))
			# drop the temporary table created
			sql_stmt = "DROP TABLE %s" %temp_table_name
			logMsg(2, 'Drop SQL Statement - %s in makeSLASummaryReport' %sql_stmt, self.module_name)
			DatabaseServer.executeSQLStatement(sql_stmt)
		topn = safe_int(report_dict.get('topn'))
		# Form the report display data
		index = 1
		for obj in objs:
			obj['sno'] = index
			report_link = 'report_type=trend&statids=%s&resid=%s&business_hr=%s&%s'%(obj.get('statid'), obj.get('resid'), business_hr, self.getSelectedTime(report_dict.get('request')))
			res_name = report_dict.get('res_path_map', {}).get(safe_int(obj.get('resid')))
			obj['resource_name'] = res_name
			obj['_resource_name'] = XMLLink('javascript:openAdHocReport(&quot;selectgroupreport?%s&quot;)' %(report_link), res_name, Title=res_name, classId='commons1')
			unit = self.statid_unit_map.get(safe_int(obj.get('statid')))
			obj['_avg'] = '%.2f %s' %(obj.get('avg'), unit)
			obj['_min'] = '%.2f %s' %(obj.get('min'), unit)
			obj['_max'] = '%.2f %s' %(obj.get('max'), unit)
			obj['sla'] = '%.2f %%' %(obj.get('comply_percentage'))
			# red color
			color_index = 0
			if safe_int(obj.get('fail_count')):
				# green color
				color_index = 2
			obj['_sla'] = [XMLImage(self.severityMap[color_index], border=0), XMLSpace(1), '%.2f %%' %(obj.get('comply_percentage'))]
			index += 1
		width = report_dict.get('width', 420)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY:
			report_dict['no_resource'] = len(objs)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)

	def makeSLADetailReport(self, report_dict):
		self.updateGroupInfo(report_dict)
		headings = [
						('sno', 'S.No', '5%', 'left'), 
						('resource_name', 'Resource Name', '30%', 'left'), 
						('_timeStamp', 'Time', '10%', 'left'), 
						('_avg', 'Value', '10%', 'left'), 
						('_sla', 'SLA Complaince', '10%', 'left'),
					]
		csv_headings = [
						('sno', 'S.No', 25), ('resource_name', 'Resource Name', 75),
						('_timeStamp', 'Time', 20), ('_avg', 'Value', 20), 
						('sla', 'SLA %', 20)
					]
		objs = []
		start_time = report_dict['st']
		end_time = report_dict['et']
		if (end_time - start_time) > ONE_DAY:
			time_format = '%m/%d %H:%M:%S'
		else:
			time_format = '%H:%M:%S'
		temp_table_name = self.doSLAComputation(report_dict)
		# change the topn option to show all.
		report_dict['topn'] = 0
		business_hr = safe_int(report_dict.get('business_hr', ''))
		if temp_table_name:
			# sql_stmt = "SELECT resid, statid, timeStamp, timeStamp AS time_stamp, avg, comply*100 AS comply_percentage FROM %s ORDER BY resid, timeStamp" %temp_table_name
			sql_stmt = "SELECT resid, statid, timeStamp, timeStamp AS time_stamp, avg, comply*100 AS comply_percentage FROM %s WHERE comply != 1 ORDER BY resid, timeStamp" %temp_table_name
			logMsg(2, 'SQL Statement - %s in makeSLADetailReport' %sql_stmt, self.module_name)
			objs = DatabaseServer.executeSQLStatement(sql_stmt)
			# if business or non business hour option is selected.. filter the data as per the selection
			if business_hr:
				objs = self.checkForBusinessHours(report_dict, objs)
			# drop the temporary table created
			sql_stmt = "DROP TABLE %s" %temp_table_name
			logMsg(2, 'Drop SQL Statement - %s in makeSLADetailReport' %sql_stmt, self.module_name)
			DatabaseServer.executeSQLStatement(sql_stmt)
		# Form the report display data
		index = 1
		for obj in objs:
			obj['sno'] = index
			obj['resource_name'] = report_dict.get('res_path_map', {}).get(safe_int(obj.get('resid')))
			obj['_timeStamp'] = time.strftime(time_format, time.localtime(safe_int(obj.get('time_stamp'))))
			unit = self.statid_unit_map.get(safe_int(obj.get('statid')))
			obj['_avg'] = '%.2f %s' %(obj.get('avg'), unit)
			# red color
			color_index = 2
			if safe_int(obj.get('comply_percentage')):
				# green color
				color_index = 0
			obj['sla'] = '%.2f %%' %(obj.get('comply_percentage'))
			obj['_sla'] = [XMLImage(self.severityMap[color_index], border=0), XMLSpace(1), '%.2f %%' %(obj.get('comply_percentage'))]
			index += 1
		width = report_dict.get('width', 420)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY:
			report_dict['no_resource'] = len(objs)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)

	def getGroupReportDatas(self, report_dict, group_type):
		""" Get the report datas from the db for each group.
		This function is being called by the group report functions
		"""
		res_path_map = report_dict.get('res_path_map', {})
		if report_dict.get('realtime'):
			objs = self.getRealTimeData(report_dict)
		else:
			objs = self.getDatasFromDB(report_dict, group_type)
		if len(objs) > 0:
			objs = self.checkForBusinessHours(report_dict, objs)
			#Added To Support FCL
			if report_dict.get('report_type') in self.node_summary_tables:
				self.getReportDataDict(objs, report_dict, group_id='all')
			else:
				self.getReportDataDict(objs, report_dict, group_id=self.group_id_map.get(group_type, 'statid'))
			#Added To Support FCL
			max_value = safe_float(report_dict.get('max_value'))
			division_factor, unit_prefix = self.formatUnit(max_value, report_dict.get('division', 1000))
			if report_dict.get('report_type').find('mrtg')!=-1:
				unit_prefix = 'K'
				division_factor = 1000
			#print division_factor, unit_prefix
			report_dict['division_factor'] = division_factor
			report_dict['unit_prefix'] = unit_prefix
			report_dict['max_value'] = safe_float(max_value / division_factor)
			if report_dict.get('report_type') == 'current_history':
				group_id = self.group_id_map.get(group_type, 'statid')
				self.getHistoryDatas(group_id, report_dict)
		else:
			report_dict['sub_report_data_dict'] = {}
		self.updateGroupInfo(report_dict)

	def getRealTimeData(self, request):
		resids = request.get('res_path_map', {}).keys()
		statids = map(lambda a: safe_int(a), request.get('statids'))
		objs = self.real_time_server.getData(resids, statids)
		for obj in objs:
			obj['time_stamp'] = safe_int(obj.get('timeStamp'))
		return objs

	def getDatasFromDB(self, report_dict, group_type, folder_summary=0, selected_resids=[]):
		"""Get datas from DB
		"""
		st_time = time.time()
		report_type = report_dict.get('report_type')
		top = safe_int(report_dict.get('topn', 4))
		resolution = report_dict.get('resolution')
		if safe_int(report_dict.get('timescale')) in [3,4] and report_dict.get('url') == 'report':
			resolution='raw'
		start_time = report_dict['st']
		end_time = report_dict['et']
		resids = report_dict['res_path_map'].keys()
		statids = unique(report_dict['report_obj'].get('statids', []))
		polarity = safe_int(report_dict['report_obj'].get('pval', 1))
		if not polarity:
			polarity = -1
		report_dict['polarity'] = polarity
		high_resolution = report_dict.get('high_resolution')
		display = report_dict.get('display','avg')
		top_resids = []
		display_str = ''
		actual_resolution = [resolution]
		flag = 0
		while not top_resids:
			tables, high_resolution_tables = self.getTables(start_time, end_time, resolution, high_resolution)
			if len(tables) == 0:
				return []
			top_resids = self.getTopData(high_resolution_tables, group_type, statids, resids, start_time, end_time, top, polarity=polarity, display_type = display,report_dict = report_dict)
			if top_resids:
				break
			if high_resolution == 'raw' and not top_resids:
				break
			if high_resolution == resolution and not top_resids:
				if resolution not in actual_resolution:
					actual_resolution.append(resolution)
				resolution = getReportParams(report_dict).low_resolution_map.get(resolution, resolution)
				flag = 1
			high_resolution = getReportParams(report_dict).low_resolution_map.get(high_resolution, high_resolution)
			report_dict['resolution_changed'] = 1
		# Folder Summary Report
		if folder_summary and selected_resids :	 
			top_resids = selected_resids
		report_dict['resolution'] = resolution
		report_dict['high_resolution'] = high_resolution
		report_dict['request']['resolution'] = resolution
		report_dict['top_resids'] = top_resids
		report_dict['statids'] = statids
		report_dict['tables'] = tables
		if flag:
			try:
				display_str = interText.interString['S5213'] %(string.join(actual_resolution, ' and '), iif(resolution == 'raw', 'minute', resolution))
			except:
				display_str = 'Data is not available in `%s` resolutions. So taking it from `%s` resolution' %(string.join(actual_resolution, ' and '), iif(resolution == 'raw', 'minute', resolution))
			report_dict['display_str'] = display_str
		lower_resolution_map = {'month': 'day', 'day': 'hour', 'hour': 'raw', 'raw': 'raw'}
		if report_type == 'current_future95':
			lower_resolution = lower_resolution_map.get(resolution, 'day')
			if self.db_map.has_key(lower_resolution):
				tables = self.db_map.get(lower_resolution).getTables(start_time, end_time-1)
			all_data = self.getAllObjs(tables, top_resids, statids, start_time, end_time, lower_resolution, report_type, top, report_dict)
		else:
			all_data = self.getAllObjs(tables, top_resids, statids, start_time, end_time, resolution, report_type, top, report_dict)
		et_time = time.time()
		logMsg(2, 'Time taken to get Datas From DB - %s: objs - %s' %(`et_time - st_time`, len(all_data)), self.module_name)
		return all_data

	def getTables(self, start_time, end_time, resolution, high_resolution):
		"""Get the tables which are need to query to get the datas 
		for the given time range and based on the resolution.
		"""
		tables = []
		## For Simulation -- Temporary Purpose.
		if maxInputStats.isSimulation:
			tables = maxInputStats.reportSimulationTables
		else:
			if self.db_map.has_key(resolution):
				tables = self.db_map.get(resolution).getTables(start_time, end_time-1)
		# No data available in that time for that resolution.
		if len(tables) == 0:
			return ([], None)
		high_resolution_tables = self.db_map.get(high_resolution).getTables(start_time, end_time-1)
		if high_resolution_tables:
			return tables, high_resolution_tables
		return tables, tables

	def getTopData(self, tables, group_type, statids, resids, start_time, end_time, top, polarity=1, display_type = "avg",report_dict = {}):
		"""Get the top data from the given table for the given statids & resids for that given period
		based on the group type.
		"""
		db_type = getDBType()
		DatabaseServer = getDBInstance()
		st_time = time.time()
		sql_stmts = []
		statids  = self.changeDerivedStatids(statids)
		sql = timeBetween(start_time, end_time,report_dict.get('timebetween',''))
		for table in tables:
			#sql_stmts.append('SELECT * FROM %s WHERE statid IN (%s) AND resid in (%s) AND timeStamp between %s AND %s' %(table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time))
			sql_stmts.append('SELECT s.* FROM %s s, (%s) r WHERE s.statid IN (%s) AND s.resid = r.resid AND s.timeStamp BETWEEN %s AND %s %s '%(table, report_dict.get('res_query'), str(statids)[1:-1], start_time, end_time, sql))
		logMsg(2, sql_stmts)
		# get top datas based on polarity
		asc_desc = iif(polarity > 0, 'DESC', 'ASC')
		if top:
			# Statistical Split
			if group_type == 0:
				#Get the top data from the high resolution table.
				#num_var = getUniqueString()
				#stat_var = getUniqueString()
				#if db_type == 'MySQL':
				#	sql_stmt = 'SET @%s=0, @%s=0;'%(num_var, stat_var)
				# 	DatabaseServer.executeSQLStatement(sql_stmt)
				# 	sql_stmt = 'SELECT resid, statid, avg FROM (SELECT x.resid, x.statid, x.avg, @%s := IF(@%s = x.statid, @%s + 1, 1) AS row_number, @%s := x.statid AS dummy FROM (SELECT resid, statid, AVG(avg) as avg from %s WHERE statid IN (%s) AND resid in (%s) AND timeStamp between %s AND %s GROUP BY resid, statid ORDER BY statid, avg DESC) x) y WHERE y.row_number <= %d' %(num_var, stat_var, num_var, stat_var, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, top)
				# 	top_objs = DatabaseServer.executeSQLStatement(sql_stmt, module=self.dblog_module)
				# 	top_resids = {}
				# 	for obj in top_objs:
				# 		if not top_resids.has_key(safe_int(obj.get('statid'))):
				# 			top_resids[safe_int(obj.get('statid'))] = []
				# 		top_resids[safe_int(obj.get('statid'))].append(safe_int(obj.get('resid')))
				#elif db_type == 'MSSQL':
				#sql_stmt = 'SELECT resid, statid, AVG(avg) as avg from %s WHERE statid IN (%s) AND resid in (%s) AND timeStamp between %s AND %s GROUP BY resid, statid ORDER BY statid, avg DESC' %(table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time)
				sql_stmt = 'SELECT x.resid, x.statid, AVG(x.avg) as avg from (%s) x GROUP BY x.resid, x.statid ORDER BY statid, avg %s' %(string.join(sql_stmts, ' UNION ALL '), asc_desc)
				#sql_stmt = queryStandardizer.getQuery('Q659') %(table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time)
				logMsg(2, 'Getting Top Data Statistical Split - %s' %(sql_stmt), self.module_name)
				top_all_objs = DatabaseServer.executeSQLStatement(sql_stmt)
				top_stat_objs = {}
				if top_all_objs != [] and top_all_objs != -1:
					for obj in top_all_objs:
						if not top_stat_objs.has_key(safe_int(obj.get('statid'))):
							top_stat_objs[safe_int(obj.get('statid'))] = []
						top_stat_objs[safe_int(obj.get('statid'))].append((safe_int(obj.get('resid')), safe_float(obj.get('avg'))))
				top_resids = {}
				for stats, resids in top_stat_objs.items():
					res_ids = map(lambda a: a[0], resids)
					# get top datas based on polarity
					top_resids[stats] = res_ids[:top]
			# Resource Split
			elif group_type == 1:
				columns_to_query = 'x.resid'
				# Get the top data from the high resolution table.
				if db_type == 'MySQL' or db_type == 'PostgreSQL':
					#sql_stmt = 'SELECT %s, AVG(avg) avg FROM %s WHERE statid IN (%s) AND resid IN (%s) AND timeStamp BETWEEN %s AND %s GROUP BY %s ORDER BY avg DESC LIMIT %d' %(columns_to_query, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, columns_to_query, top)
					#sql_stmt = queryStandardizer.getQuery('Q660') %(columns_to_query, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, columns_to_query, top)
					sql_stmt = 'SELECT %s, AVG(x.avg) avg FROM (%s) x GROUP BY %s ORDER BY avg %s LIMIT %d' %(columns_to_query, string.join(sql_stmts, ' UNION ALL '), columns_to_query, asc_desc, top)
				elif db_type == 'MSSQL':
					#sql_stmt = 'SELECT TOP %s %s, AVG(avg) avg FROM %s WHERE statid IN (%s) AND resid IN (%s) AND timeStamp BETWEEN %s AND %s GROUP BY %s ORDER BY avg DESC' %(top, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time)
					#sql_stmt = queryStandardizer.getQuery('Q660') %(top, columns_to_query, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, columns_to_query)
					sql_stmt = 'SELECT TOP %s %s, AVG(x.avg) avg FROM (%s) x GROUP BY %s ORDER BY avg %s' %(top, columns_to_query, string.join(sql_stmts, ' UNION ALL '), columns_to_query, asc_desc)
				elif db_type == 'ORACLE':
					#sql_stmt = 'SELECT %s, AVG(avg) avg FROM %s WHERE statid IN (%s) AND resid IN (%s) AND timeStamp BETWEEN %s AND %s AND ROWNUM <= %s GROUP BY %s ORDER BY avg DESC' %(columns_to_query, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, columns_to_query, top)
					#sql_stmt = queryStandardizer.getQuery('Q660') %(columns_to_query, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, columns_to_query, top)
					sql_stmt = 'SELECT %s, AVG(x.avg) avg FROM (%s) x GROUP BY %s ORDER BY avg %s LIMIT %d' %(columns_to_query, string.join(sql_stmts, ' UNION ALL '), columns_to_query, asc_desc, top)
				logMsg(2, 'Getting Top Data Resource Split - %s' %(sql_stmt), self.module_name)
				top_objs = DatabaseServer.executeSQLStatement(sql_stmt, module=self.dblog_module)
				top_resids = map(lambda a: safe_int(a.get('resid')), top_objs)
			# Single & Multiple Graph
			else:# elif group_type == 2 and group_type == 3:
				columns_to_query = 'x.resid, x.statid'
				# Get the top data from the high resolution table.
				if db_type == 'MySQL' or db_type == 'PostgreSQL':
					#sql_stmt = 'SELECT %s, AVG(avg) avg FROM %s WHERE statid IN (%s) AND resid IN (%s) AND timeStamp BETWEEN %s AND %s GROUP BY %s ORDER BY avg DESC LIMIT %d' %(columns_to_query, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, columns_to_query, top)
					#sql_stmt = queryStandardizer.getQuery('Q660') %(columns_to_query, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, columns_to_query, top)
					sql_stmt = 'SELECT %s, AVG(x.avg) avg FROM (%s) x GROUP BY %s ORDER BY avg %s LIMIT %d' %(columns_to_query, string.join(sql_stmts, ' UNION ALL '), columns_to_query, asc_desc, top)
				elif db_type == 'MSSQL':
					#sql_stmt = 'SELECT TOP %s %s, AVG(avg) avg FROM %s WHERE statid IN (%s) AND resid IN (%s) AND timeStamp BETWEEN %s AND %s GROUP BY %s ORDER BY avg DESC' %(top, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time)
					#sql_stmt = queryStandardizer.getQuery('Q660') %(top, columns_to_query, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, columns_to_query)
					sql_stmt = 'SELECT TOP %s %s, AVG(x.avg) avg FROM (%s) x GROUP BY %s ORDER BY avg %s' %(top, columns_to_query, string.join(sql_stmts, ' UNION ALL '), columns_to_query, asc_desc)
				elif db_type == 'ORACLE':
					#sql_stmt = 'SELECT %s, AVG(avg) avg FROM %s WHERE statid IN (%s) AND resid IN (%s) AND timeStamp BETWEEN %s AND %s AND ROWNUM <= %s GROUP BY %s ORDER BY avg DESC' %(columns_to_query, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, columns_to_query, top)
					#sql_stmt = queryStandardizer.getQuery('Q660') %(columns_to_query, table, str(statids)[1:-1], str(resids)[1:-1], start_time, end_time, columns_to_query, top)
					sql_stmt = 'SELECT %s, AVG(x.avg) avg FROM (%s) x GROUP BY %s ORDER BY avg %s LIMIT %d' %(columns_to_query, string.join(sql_stmts, ' UNION ALL '), columns_to_query, asc_desc, top)
				logMsg(2, 'Getting Top Data Single & Multiple Graph - %s' %(sql_stmt), self.module_name)
				top_objs = DatabaseServer.executeSQLStatement(sql_stmt, module=self.dblog_module)
				top_resids = map(lambda a: (safe_int(a.get('statid')), safe_int(a.get('resid'))), top_objs)
		else:
			# Statistical Split
			if group_type == 0:
				top_resids = {}
				for statid in statids:
					top_resids[statid] = resids
			# Resource Split
			elif group_type == 1:
				top_resids = resids
			# Single & Multiple Graph
			else:
				top_resids = []
				for statid in statids:
					for resid in resids:
						top_resids.append((statid, resid))
				top_resids = distinct(top_resids)
		et_time = time.time()
		logMsg(2, 'Time taken to get the top data : %s' %(et_time - st_time), self.module_name)
		return top_resids		

	def changeDerivedStatids(self, statids):
		new_statids = []
		for statid in statids:
			if statid in range(-1,-13,-1):
				new_statids.append(self.statname_id_map.get('Outage'))
			elif statid in range(-13,-20,-1):
				new_statids.append(self.statname_id_map.get('Bussiness Outage'))
			else:
				new_statids.append(statid)
		return new_statids
	
	def getAllObjs(self, tables, resids, statids, start_time, end_time, resolution, report_type, top, report_dict):
		"""Get all the objs from the tables.
		"""
		objs = []
		non_derived_stats = filter(lambda a: safe_int(a) not in range(-1,-20,-1), statids[:])
		derived_alltime_stats = filter(lambda a: safe_int(a) in range(-1,-13,-1) , statids[:])
		derived_bussiness_stats = filter(lambda a: safe_int(a) in range(-13,-20,-1), statids[:])
		if non_derived_stats:
			non_dervived_objs = self.getObjs(tables, resids, non_derived_stats, start_time, end_time, resolution, report_type, top, report_dict)
			if non_dervived_objs and type(non_dervived_objs) == type([]):
				objs = non_dervived_objs[:]
		if derived_alltime_stats:
			derived_alltime_objs = self.getObjs(tables, resids, unique(self.changeDerivedStatids(derived_alltime_stats)), start_time, end_time, resolution, report_type, top, report_dict)
			if derived_alltime_objs and type(derived_alltime_objs) == type([]):
				self.covertDerviedToRegularObjs(derived_alltime_objs, resids, statids, start_time, end_time, resolution, report_type, top, report_dict, objs)
		if derived_bussiness_stats:
			derived_bussiness_objs = self.getObjs(tables, resids, unique(self.changeDerivedStatids(derived_bussiness_stats)), start_time, end_time, resolution, report_type, top, report_dict)
			if derived_bussiness_objs and type(derived_bussiness_objs) == type([]):
				self.covertDerviedToRegularObjs(derived_bussiness_objs, resids, statids, start_time, end_time, resolution, report_type, top, report_dict, objs)
		logMsg(2, "non_derived_stats = %s , derived_alltime_stats = %s , derived_bussiness_stats= %s"%(non_derived_stats,derived_alltime_stats,derived_bussiness_stats),self.module_name)
		logMsg(2, "Total objs len  = %d "%(len(objs)),self.module_name)
		return objs

	def getObjs(self, tables, resids, statids, start_time, end_time, resolution, report_type, top, report_dict):
		DatabaseServer = getDBInstance()
		st_time = time.time()
		all_objs = []
		if len(resids) > 0:
			# if top option is selected.
			if top:
				if type(resids) == types.ListType:
					# For Single & Multiple Graph
					if type(resids[0]) == types.TupleType:
						filter_condn = '(%s)' %(string.join(map(lambda a, b = '(statid = %s AND resid = %s)': b %(str(a[0]), str(a[1])), resids), ' OR '))
					# For Resource Split
					else:
						filter_condn = 'statid IN (%s) AND resid IN (%s)' %(str(statids)[1:-1], str(resids)[1:-1])
				# For Statistical Split
				else:
					filter_condn = '(%s)' %(string.join(map(lambda a, b = '(statid = %s AND resid IN (%s))': b %(str(a[0]), str(a[1])[1:-1]), resids.items()), ' OR '))
			# show all option is selected.
			else:
				filter_condn = 'statid IN (%s)' %(str(statids)[1:-1])
				logMsg(2, 'Since show all option is selected... get the data based on statids - %s' %(filter_condn), self.module_name)
			raw_aggregation = 60
			if report_dict.get('raw_resolution_aggregation'):
				raw_aggregation = safe_int(report_dict.get('raw_resolution_aggregation'))			
			if resolution == 'raw' and report_type != 'data' and report_type != 'trend':
				timeStamp = '(timeStamp - (timeStamp %% %s))' %(raw_aggregation)
				db_type = getDBType()
				if db_type == 'MYSQL':
					sql_stmt = 'SELECT resid, statid, %s time_stamp, AVG(avg) avg FROM %s WHERE %s AND timeStamp between %s AND %s %s GROUP BY resid, statid, time_stamp ORDER BY time_stamp. resid, statid'
				else:
					sql_stmt = 'SELECT resid, statid, %s time_stamp, AVG(avg) avg FROM %s WHERE %s AND timeStamp between %s AND %s %s GROUP BY resid, statid, timeStamp ORDER BY time_stamp, resid, statid'
			else:
				timeStamp = 'timeStamp'
				if resolution == 'raw' :
					sql_stmt = "SELECT resid, statid, %s time_stamp, avg, avg as min, avg as max FROM %s WHERE %s AND timeStamp BETWEEN %s AND %s %s ORDER BY time_stamp, resid, statid"
				else :
					sql_stmt = "SELECT resid, statid, %s time_stamp, avg, min as min, max as max FROM %s WHERE %s AND timeStamp BETWEEN %s AND %s %s ORDER BY time_stamp, resid, statid"
			sql = timeBetween(start_time, end_time,report_dict.get('timebetween',''))
			for table in tables:
				prepared_sql_stmt = sql_stmt %(timeStamp, table, filter_condn, start_time, end_time, sql)
				logMsg(2, 'Getting All Data - %s' %(prepared_sql_stmt), self.module_name)
				objs = DatabaseServer.executeSQLStatement(prepared_sql_stmt, module=self.dblog_module)
				if objs != [] and objs != -1:
					if report_type == 'current_future95':
						objs = self.calculatePercentile(report_dict, objs, resolution)
					all_objs += objs
		et_time = time.time()
		logMsg(4, 'Time taken to get the all data - %s : objs - %s' %(`et_time - st_time`, len(all_objs)), self.module_name)
		return all_objs

	def calculatePercentile(self, report_dict, objs, resolution):
		resolution_map = {'day': 'month', 'hour': 'day', 'raw': 'hour'}
		original_resolution = report_dict.get('resolution', '')
		if original_resolution == resolution and resolution == 'raw':
			return objs
		higher_resolution = resolution_map.get(resolution)
		min_time = numpy.min(map(lambda a: safe_int(a.get('time_stamp')), objs))
		if higher_resolution == 'month':
			time_format = '%Y-%m-01 00:00:00'
		elif higher_resolution == 'day':
			time_format = '%Y-%m-%d 00:00:00'
		elif higher_resolution == 'hour':
			time_format = '%Y-%m-%d %H:00:00'
		timeStamp = timeStrTotimeInt(time.strftime(time_format, time.localtime(min_time)))
		res_stat_objs = classifyObjs(objs, lambda a: (safe_int(a.get('resid')), safe_int(a.get('statid'))))
		calculated_objs = []
		for (resid, statid), rs_objs in res_stat_objs.items():
			calculated_obj = {}
			calculated_obj['resid'] = resid
			calculated_obj['statid'] = statid
			calculated_obj['time_stamp'] = timeStamp
			avg_values = map(lambda a: safe_float(a.get('avg')), rs_objs)
			percentile_95 = numpy.percentile(avg_values, 95)
			calculated_obj['avg'] = '%.3f' %percentile_95
			calculated_objs.append(calculated_obj)
		return calculated_objs

	def covertDerviedToRegularObjs(self,in_objs, resids, statids, start_time, end_time, resolution, report_type, top, report_dict, objs):
		try:
			for each_statid in statids:
				groupbyresid = classifyObjs(in_objs, lambda a : (a.get("resid", ""), a.get("statid", "")))
				for ((resid_, statid_), classified_objs) in groupbyresid.items() :
					classified_objs.sort(lambda a, b, c = [(1,'time_stamp')] : cmpField(a, b, c))
					for obj in classified_objs:
						resid  = safe_int(obj.get('resid'))
						ip = report_dict['res_hostip_map'].get(resid,'')
						if ip == '':
							logMsg(2,"No ip to resid %d so reject this obj"%resid,self.module_name)
							continue
						current_value = safe_float(obj['avg'])
						current_time = safe_float(obj.get('time_stamp'))
						today_start_time = safe_float(timeStrTotimeInt(time.strftime('%Y-%m-%d 00:00:00', time.localtime(current_time))))
						if classified_objs.index(obj) != 0:
							previous_value = safe_float(classified_objs[classified_objs.index(obj) - 1]['avg'])
							previous_time = safe_float(classified_objs[classified_objs.index(obj) - 1]['time_stamp'])
						obj['statid'] = safe_int(each_statid)
						new_obj = copy.deepcopy(obj)
						#Down Time hrs, Down time mins, Down time Production hrs,Down time Production mins 
						if obj['statid'] in [-1, -6, -13, -15, -10]: 
							if resolution in ['raw','hour']:
								if classified_objs.index(obj) != 0:
									avg = current_value - previous_value
									if avg < 0:
										avg = current_value
									new_obj['avg'] = math.floor(avg)
							else:
								#Planned Down Time
								if obj['statid'] in [-10]:
									maintenancetime = self.getMaintenanceHrs(ip, current_time, resolution)
								elif obj['statid'] in [-13,-15]:
									production_hrs = self.getProductionHours(current_time, resolution, 0)
									if production_hrs == 0:
										logMsg(2,"Production hour value is zero",self.module_name)
										continue
									maintenancetime = self.getBusinessMaintenanceHrs(ip, current_time, resolution)
								if obj['statid'] in [-10,-13,-15]:
									avg  = current_value  + maintenancetime
									if resolution == "day":
										avg = iif(avg > 86400, 84600, avg)
									elif resolution == "month":
										loc_time = time.localtime(current_time)
										month_hours = getNumberofDays(loc_time[0],loc_time[1]) * 86400
										avg  = iif (avg > month_hours, month_hours,avg)
									new_obj['avg'] = math.floor(avg)								
						#Total UpTime hrs, Total up time mins,  Total Uptime planned hrs,Total Uptime planned mins, Total Uptime production hrs, Total Uptime production mins
						elif obj['statid'] in [-2, -7, -9, -12, -14, -16]: 
							if resolution in ['raw','hour']:
								if classified_objs.index(obj) != 0:
									avg = current_value - previous_value
									if avg < 0:
										avg = current_value
									avg = current_time - previous_time - avg
								else:
									if current_time - today_start_time <= 0:
										if resolution == "hour":
											avg = 3600 -  current_value
										else:
											if  len(classified_objs) > 1:
												avg = safe_int(classified_objs[1].get('time_stamp')) - current_time - current_value
											else:
												avg = 300 - current_value
									else:				
										avg = current_time - today_start_time - current_value
							else:
								#Add the Maintenance Time
								avg = current_value
								if obj['statid'] in [-9, -12]:
									maintenancetime = self.getMaintenanceHrs(ip, current_time, resolution )
									avg  = current_value  + maintenancetime
								if resolution == 'day':
									#For uptime production hrs , mins
									if obj['statid'] in [-14, -16]:
										production_hrs = self.getProductionHours(current_time, resolution, 0)
										if production_hrs == 0:
											logMsg(2,"Production hour value is zero",self.module_name)
											continue
										maintenancetime = self.getBusinessMaintenanceHrs(ip, current_time, resolution)
										avg  = current_value  + maintenancetime
										avg = production_hrs - iif(avg > production_hrs, production_hrs, avg)
									else:
										avg = 86400 - iif(avg > 86400, 86400, avg)
								else:
									#For uptime production hrs
									if obj['statid'] in [-14, -16]:
										production_hrs = self.getProductionHours(current_time, resolution, 0)
										if production_hrs == 0:
											logMsg(2,"Production hour value is zero",self.module_name)
											continue
										maintenancetime = self.getBusinessMaintenanceHrs(ip, current_time, resolution)
										avg  = current_value  + maintenancetime
										avg = production_hrs - iif(avg > production_hrs, production_hrs, avg)
									else:
										loc_time = time.localtime(current_time)
										month_hours = getNumberofDays(loc_time[0],loc_time[1]) * 86400
										avg  = month_hours - iif (avg > month_hours, month_hours,avg)
							new_obj['avg'] = math.floor(avg)
						#Down Time %, UpTime %, UpTime% Planned, Uptime Production %, down time production %
						elif obj['statid'] in [-4, -3,-11, -17 ,-18, -8]: 
							if resolution in ['raw','hour']:
								if classified_objs.index(obj) != 0:
									if current_value - previous_value < 0:
										avg = ((current_value) /(current_time - previous_time)) * 100
									else:
										if current_time - previous_time <= 0:
											avg = ((current_value - previous_value)) * 100
										else:	
											avg = ((current_value - previous_value) /(current_time - previous_time)) * 100
								else:
									if safe_int(current_time - today_start_time) <= 0 :
										avg = 0
										if current_value:
											avg = 100
									else:
										avg = ((current_value) /(current_time - today_start_time)) * 100
							else:
								#Add the Maintenance Time
								if obj['statid'] in [-11, -8]:
									maintenancetime = self.getMaintenanceHrs(ip, current_time, resolution)
									current_value += maintenancetime
								if resolution == 'day':
									if obj['statid'] in [-17, -18]:
										production_hrs = self.getProductionHours(current_time, resolution, 0)
										if production_hrs == 0:
											logMsg(2,"Production hour value is zero",self.module_name)
											continue
										maintenancetime = self.getBusinessMaintenanceHrs(ip, current_time, resolution)
										current_value  += maintenancetime
										avg = (current_value / production_hrs) * 100
									else:
										avg = (current_value / 86400) * 100
								else:
									if obj['statid'] in [-17, -18]:
										production_hrs = self.getProductionHours(current_time, resolution, 0)
										if production_hrs == 0:
											logMsg(2,"Production hour value is zero",self.module_name)
											continue
										maintenancetime = self.getBusinessMaintenanceHrs(ip, current_time, resolution)
										current_value  += maintenancetime
										avg = (current_value / production_hrs) * 100
									else:
										loc_time = time.localtime(current_time)
										month_hours = getNumberofDays(loc_time[0],loc_time[1]) * 86400
										avg = (current_value / month_hours) * 100
							avg = iif(avg > 100, 100, avg)
							#For Up Time %
							if obj['statid'] in [ -3,-11, -17]:
								avg = 100 - avg	
							new_obj['avg'] = '%.2f'%avg
						#Production Hours Production mins
						elif obj['statid'] in [-19]: 
							if resolution in ['raw']:
								if classified_objs.index(obj) != 0:
									production_hrs = self.getProductionHours(current_time, resolution, current_time - previous_time)
								else:
									production_hrs = self.getProductionHours(current_time, resolution, 300)
							else:
								production_hrs = self.getProductionHours(current_time, resolution, 0)
							if production_hrs == 0:
								logMsg(2,"Production hour value is zero",self.module_name)
								continue
							avg = production_hrs
							new_obj['avg'] = avg
						objs.append(new_obj)
		except Exception, msg:
			logExceptionMsg(4, 'Exception  - in covertDerviedToRegularObjs %s' %(msg), self.module_name)

	def checkForBusinessHours(self,report_dict,objs):
		"""Filter the Objs for the Business Hour Selection
		"""
		if safe_int(report_dict.get("business_hr",0)) != 0 :
			logMsg(4,'Before checkForBusinessHours - %s' %(len(objs)), self.module_name)
			holidayDates = self.businessHrServer.getHolidayObj()
			profile_dn = report_dict.get("business_hr_profile",'')
			if not profile_dn:
				bus_objs = self.businessHrServer.getBusObjs()
			else:
				bus_objs = self.businessHrServer.getBusObjs(profile_dn)
			isBusinessHr = {"sh":safe_int(report_dict.get("business_hr",0))}
			logMsg(4,'Inside checkForBusinessHours function - Actual Objs: %s' %len(objs), self.module_name)
			logMsg(4,'Inside checkForBusinessHours function - bus_objs: %s, holidayDates: %s, isBusinessHr: %s' %(bus_objs, holidayDates, isBusinessHr), self.module_name)
			newObjs = filter(lambda a: isPresentTimeInPollTimeReport(a.get("time_stamp"),isBusinessHr, holidayDates = holidayDates, objs = bus_objs)  ,objs)
			logMsg(4,'Returning checkForBusinessHours Filtered - newObjs: %s' %len(newObjs), self.module_name)
			return newObjs
		return objs

	def getReportDataDict(self, objs, report_dict, group_id='statid', history=0):
		"""Form the report data dict from the objs.
		Format of the data structure is {statid1: {path1: [(timestamp1, data1), (timestamp2, data2)...]..}..}
		"""
		st_time = time.time()
		if report_dict.get('display', 'avg') == 'all' or report_dict.get("report_type") in self.node_summary_details :
			legends = ['min', 'max', 'avg']
		else:
			display = report_dict.get('display', 'avg')
			if not display:
				display = 'avg'
			if display not in ['avg', 'min', 'max']:
				display = 'avg'
			legends = [display]
		try:
			legends_map = {'min': interText.interLink['L205'], 'max': interText.interString['S5214'], 'avg': interText.interLink['L203']}
		except:
			legends_map = {'min': 'Minimum', 'max': 'Maximum', 'avg': 'Average'}
		max_value = 0.0
		sub_report_data_dict = {}
		legend_unit_map = {}
		path_resid_map = {}
		res_path_map = report_dict.get('res_path_map', {})
		res_actual_path_map = report_dict.get('res_actual_path_map', {})
		res_hostip_map = report_dict.get('res_hostip_map', {})
		title_map = {}
		start_time, end_time, time_gap = self.getTimeRange(report_dict)
		report_time_stamps = report_dict.get('report_time_stamps', {})
		legend_report_link_map = {}
		# Filter the objs if show all option is selected.
		if safe_int(report_dict.get('topn')) == 0:
			objs = filter(lambda a, b=res_actual_path_map: b.has_key(safe_int(a.get('resid'))), objs)
		# Form the report data dict
		red_val = safe_float(report_dict.get('report_obj', {}).get('red_val'))
		yellow_val = safe_float(report_dict.get('report_obj', {}).get('yellow_val'))
		temp_report_link = 'group_type=%s&red_val=%s&yellow_val=%s&report_type=trend' %(safe_int(report_dict.get('group_type')), red_val, yellow_val)
		for obj in objs:
			for legend in legends:
				statid = safe_int(obj.get('statid'))
				resid = safe_int(obj.get('resid'))
				report_link = 'statids=' + `statid` + '&pathnames=' + res_actual_path_map.get(resid, '') + '&' + temp_report_link + '&' + "poll_addr=%s&resid=%d"%(res_hostip_map.get(resid,''),resid) 
				stat_name = changeOption(self.statid_name_map.get(statid))
				path = stat_name + '(' + str(res_path_map.get(resid)) + ')'
				path_resid_map[str(res_path_map.get(resid))] = resid
				if maxInputStats.AVG_MIN_MAX_IN_REPORT or report_dict.get("report_type") in self.node_summary_details :
					path = legends_map.get(legend, '') + ' ' + path
					report_link += '&display=%s' %(legend)
				y_title = stat_name
				chart_title = ''
				if group_id == 'resid':# resource split
					groupid = resid
					path = stat_name
					# Changing Path Display only for resource split
					if maxInputStats.AVG_MIN_MAX_IN_REPORT:
						if not (len(legends) == 1 and display == 'avg'):
							path = legends_map.get(legend, '') + ' ' + path
					y_title = ''
					chart_title = str(res_path_map.get(resid))
				elif group_id == 'statid':# statistical split
					groupid = statid
					path = str(res_path_map.get(resid))
					#if report_dict.get('report_type', '') in ['data', 'summary']:
					chart_title = stat_name
					y_title = ''
				elif group_id == 'resid_statid':# multiple graph
					groupid = (statid, resid)
					path = str(res_path_map.get(resid))
					if report_dict.get('report_type', '') in ['data', 'summary']:
						chart_title = stat_name
				elif group_id == 'all': #Show all the stats in a Group				
					groupid = -1
				else:# single graph
					groupid = self.statid_unit_map.get(statid)
					y_title = ''
				if not title_map.has_key(groupid):
					title_map[groupid] = {'y_title': y_title, 'chart_title': chart_title} 
					#title_map[groupid] = y_title
				value = safe_float(obj.get(legend))
				if not sub_report_data_dict.has_key(groupid):
					sub_report_data_dict[groupid] = {}
					legend_unit_map[groupid] = {}
					legend_report_link_map[groupid] = {}
				if not sub_report_data_dict[groupid].has_key(path):
					sub_report_data_dict[groupid][path] = []
				if not legend_unit_map[groupid].has_key(path):
					legend_unit_map[groupid][path] = self.statid_unit_map.get(statid)
				if not legend_report_link_map[groupid].has_key(path):
					legend_report_link_map[groupid][path] = report_link
				sub_report_data_dict[groupid][path].append((obj.get('time_stamp'), value))
				if value > max_value:
					max_value = value
				report_dict['division'] = 1024
				if self.statid_unit_map.get(statid)[:2] == '**' or self.statid_unit_map.get(statid)[:] == 'bps':
					report_dict['division'] = 1000
		# Constructing the sub_report_data_dict, for the 
		#	report_type in self.stacked_charts or report_type is current Vs future
		#	report_type == TREND and resolution == Month
		
		if report_dict.get('report_type').find('mrtg')==-1:
			if (report_dict['report_type'] in self.stacked_charts or (report_dict['report_type'] == 'current_future') or (report_dict['report_type'] == 'current_future95') or (report_dict['report_type'] == 'trend' and report_dict['resolution'] == 'month') or report_dict.has_key('month_graph')):
				if report_dict.get('report_type','') not in ['hbar','bar','pattern','summary']:
					for groupid, legend_data_dict in sub_report_data_dict.items():
						for legend, datas in legend_data_dict.items():
							time_stamps = {}
							time_stamps.update(report_time_stamps)
							for time_stamp, data in datas:
								time_stamps[time_stamp] = data
							temp_datas = time_stamps.items()
							temp_datas.sort(sortTuple1)
							legend_data_dict[legend] = temp_datas
		if not history:
			report_dict['sub_report_data_dict'] = sub_report_data_dict
			report_dict['legend_unit_map'] = legend_unit_map
			report_dict['path_resid_map'] = path_resid_map
			report_dict['legend_report_link_map'] = legend_report_link_map
			report_dict['max_value'] = max_value
			report_dict['title_map'] = title_map
		else:
			report_dict['history_data_dict'] = sub_report_data_dict
		et_time = time.time()
		logMsg(2, 'Time taken to get Report Data Dict - %s' %(`et_time - st_time`), self.module_name)

	def getTimeRange(self, report_dict):
		"""Get the start time, end time, time gap based on 
		the selected time & resolution.
		"""
		resolution = report_dict.get('resolution', 'raw')
		report_type = report_dict.get('report_type', 'trend')
		start_time = safe_float(report_dict.get('st'))
		end_time = safe_float(report_dict.get('et'))
		diff_time = end_time - start_time
		report_dict['diff_time'] = diff_time
		st = time.localtime(start_time)
		if resolution == 'raw':
			time_gap = ONE_MINUTE
		elif resolution == 'hour':
			time_gap = ONE_HOUR
			start_time = time.mktime((st[0], st[1], st[2], st[3], 0, 0, st[6], st[7], st[8]))
		elif resolution == 'day':
			time_gap = ONE_DAY
			if (end_time - start_time) >= (10*86400):
				time_gap = 2*ONE_DAY
			start_time = time.mktime((st[0], st[1], st[2], 0, 0, 0, st[6], st[7], st[8]))
		else:# elif resolution == 'month':
			# Time Gap will be construct Dynamically, as gap is not proper
			start_time = time.mktime((st[0], st[1], 1, 0, 0, 0, st[6], st[7], st[8]))
		# selected is year
		if safe_int(report_dict.get('timescale')) == 23 or resolution == 'year':
			start_time = time.mktime((st[0], st[1], 1, 0, 0, 0, st[6], st[7], st[8]))
			# leap year
			if st[0] % 4 == 0:
				time_gap = 366 * ONE_DAY
			else:
				time_gap = 365 * ONE_DAY
		if report_dict.get('resolution_changed') or (diff_time < ONE_HOUR) or (diff_time < (3 * ONE_HOUR) and diff_time > ONE_HOUR) or report_dict.get('report_type') == 'trend':
			end_time = end_time + 1
		time_stamps = {}
		cur_time = start_time
		while cur_time < end_time:
			if resolution == 'month' :
				# Constructing Time Gap for Month
				cur_time_tuple = time.localtime(cur_time)
				NO_OF_DAYS = getReportParams(report_dict).monthToDayDict.get(cur_time_tuple[1],31)
				if cur_time_tuple[0] % 4 == 0 and cur_time_tuple[1] == 2 :
					# Increasing Feb Days to 29 for Leap Yrs
					NO_OF_DAYS = NO_OF_DAYS + 1
				time_gap = NO_OF_DAYS * ONE_DAY
			time_stamps[cur_time] = NULL_VALUE
			cur_time += time_gap
		report_dict['report_time_stamps'] = time_stamps
		if diff_time > (THIRTY_ONE_DAYS * ONE_DAY) and report_dict.get('resolution') != 'month':
			if resolution == 'raw':
				report_dict['report_type'] = 'data'
				try:
					report_dict['display_str'] = interText.interString['S5216']
				except:
					report_dict['display_str'] = 'Graph is not supported for this duration. Showing the data table report.'
			else:
				report_dict['month_graph'] = 1
		# No of days.
		multiple_factor = 1
		if resolution == 'raw': # For Raw Resolution
			# Changing the Multiple factor depends on the Range of time
			if safe_int((diff_time - 1) / (THIRTY_DAYS * ONE_DAY)):
				multiple_factor = TWENTY_FOUR_HOURS * SIXTY_MINUTES
			elif safe_int((diff_time - 1) / (ONE_DAY)):
				multiple_factor = TWENTY_FOUR_HOURS
		elif resolution == 'hour':
			if safe_int((diff_time - 1) / (THIRTY_DAYS * ONE_DAY)):
				multiple_factor = TWENTY_FOUR_HOURS
		label_step_factor = 1
		len_total_data = len(time_stamps)
		if resolution == 'raw':
			# special handling for selecting more than one month...
			# currently this option is not supported...
			if diff_time > THIRTY_ONE_DAYS * ONE_DAY:
				label_step_factor = THIRTY_ONE_DAYS * TWENTY_FOUR_HOURS * SIXTY_MINUTES
			else:
				if len_total_data > FIVE_MINUTES_IN_HOUR:
					max_entries = safe_int((diff_time + 1) / ONE_DAY)
					if not max_entries:
						max_entries = FIVE_MINUTES_IN_HOUR
					if max_entries == 1:
						max_entries = MAXIMUM_ENTRIES
					label_step_factor = safe_int(len_total_data / multiple_factor)
					label_step_factor = safe_int(label_step_factor / max_entries)
				elif len_total_data == SIXTY_MINUTES:
					label_step_factor = (SIXTY_MINUTES / FIVE_MINUTES_IN_HOUR)
		elif resolution == 'hour':
			# special handling for selecting more than one month...
			if diff_time > THIRTY_ONE_DAYS * ONE_DAY:
				label_step_factor = THIRTY_ONE_DAYS
			else:
				if len_total_data > (TWENTY_FOUR_HOURS + 1):
					# Changes made for the Bug#:9035
					if report_dict.has_key('start_time') and report_dict['start_time'] != '' and report_dict['timescale'] == '' and report_dict.get('cal_val','') == '' and len_total_data < (TWENTY_FOUR_HOURS +1) * 3:
						label_step_factor = 2
						if len_total_data > (TWENTY_FOUR_HOURS +1) * 2:
							label_step_factor = 4
					#End here
					else:
						max_entries = safe_int((diff_time + 1) / ONE_DAY)
						label_step_factor = safe_int(len_total_data / multiple_factor)
						label_step_factor = safe_int(label_step_factor / max_entries)
		elif resolution == 'day':
			if len_total_data == THIRTY_DAYS or len_total_data == THIRTY_ONE_DAYS:
				label_step_factor = 1
			else:
				if diff_time > THIRTY_ONE_DAYS * ONE_DAY:
					label_step_factor = THIRTY_ONE_DAYS
				else:
					if len_total_data > THIRTY_ONE_DAYS:
						label_step_factor = safe_int(len_total_data / THIRTY_ONE_DAYS)
					elif len_total_data > THIRTY_DAYS:
						label_step_factor = safe_int(len_total_data / THIRTY_DAYS)
		report_dict['label_step_factor'] = label_step_factor * multiple_factor
		if report_dict.get('realtime'):	
			end_time = end_time + time_gap
		return (start_time, end_time, time_gap)

	def formatUnit(self, value, division=1000):
		"""Format the unit based on the values.
		"""
		unit_prefix = ''
		division_factor = 1
		if value >= pow(division, 4):
			unit_prefix = 'T'
			division_factor = pow(division, 4) * 1.0
		elif value >= pow(division, 3):
			unit_prefix = 'G'
			division_factor = pow(division, 3) * 1.0
		#elif value >= pow(division, 2):
		#	unit_prefix = 'M'
		#	division_factor = pow(division, 2) * 1.0
		elif value >= 2599000: #Hardcoded for Airtel till 2.59 Mbs it is Kbps
			unit_prefix = 'M'
			division_factor = pow(division, 2) * 1.0
		elif value >= division:
			unit_prefix = 'K'
			division_factor = division
		elif value < 1 and value > 0:
			division_factor = 0.01
		return division_factor, unit_prefix

	def getHistoryDatas(self, group_id, report_dict):
		"""Get the history datas - Get 10 times of data history from DB for the selected duration.
		If the current report time duration is 1 hour, take the 10 hours of history data
		If the current report time duration is 1 day, take the 10 days of history data
		If the current report time duration is 1 week, take the 10 weeks of history data
		If the current report time duration is 1 month, take the 10 months of history data
		"""
		st_time = time.time()
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		top = safe_int(report_dict.get('topn', 4))
		resolution = report_dict.get('resolution')
		resids = report_dict.get('top_resids')
		statids = report_dict['report_obj'].get('statids', [])
		objs = []
		duration = end_time - start_time
		for count in range(1, self.history_count+1):
			history_start_time = start_time - (duration * count)
			history_end_time = end_time - (duration * count)
			tables = self.db_map.get(resolution).getTables(history_start_time, history_end_time-1)
			objs += self.getAllObjs(tables, resids, statids, history_start_time, history_end_time, resolution, report_dict['report_type'],top, report_dict)
		self.getReportDataDict(objs, report_dict, group_id, history=1)
		et_time = time.time()
		logMsg(2, 'Time taken to get History Datas - %s' %(`et_time - st_time`), self.module_name)

	def updateGroupInfo(self, report_dict):
		"""Update the group info with the tables, start_time, end_time, resolution, top
		report type, resids, statids values.
		"""
		report_dict['group'] = {
					# 'time_scale': report_dict.get('timescale'),
					# 'cal_val': report_dict.get('cal_val'),
					'resids': report_dict.get('top_resids'),
					'statids': report_dict.get('statids'),
					'st': report_dict.get('st'),
					'et': report_dict.get('et'),
					'tables': report_dict.get('tables'),
					'topn': report_dict.get('topn'),
					'report_type': report_dict.get('report_type'),
					'pattern_type': report_dict.get('pattern_type'),
					'resolution': report_dict.get('resolution'),
					'high_resolution': report_dict.get('high_resolution'),
					'width': report_dict.get('width'),
					'timebetween': report_dict.get('timebetween',''),
				}

	def getSortedDatas(self, datas, report_dict):
		""" Arrange the datas based on the statistics order as given in the configuration.
		This is applicable only for Statistical Split. For other group types, sort the datas.
		"""
		group_type = safe_int(report_dict.get('group_type'))
		temp_datas = []
		# sorting based on groupids not required for statistical split reports.
		if group_type == 0 and report_dict['report_type'] not in self.node_summary_tables:
			statids = report_dict.get('statids')
			for statid in statids:
				if datas.has_key(safe_int(statid)):
					temp_datas.append((safe_int(statid), datas.get(safe_int(statid))))
		else:
			temp_datas = datas.items()
			temp_datas.sort(sortTuple1)
		return temp_datas

	def checkForNullValues(self, groupid, report_data_dict, report_dict):
		"""Check for null values if data is not available for any resource on that particular time.
		If so, then add null values.
		"""
		# For Pattern Report add null values if the value is not there for that time
		if report_dict.get('report_type') == 'pattern':
			self.getPatternData(report_data_dict, report_dict)
		# Interpolate the values for the stacked charts to get similar graph like line.
		elif report_dict.get('report_type') in self.stacked_charts:
			self.interpolateOrExtrapolateValues(report_data_dict, report_dict)
		# Extrapolate the values for the future datas based on the current datas.
		elif report_dict.get('report_type') == 'current_future' or report_dict.get('report_type') == 'current_future95' :
			legend_unit_map = report_dict.get('legend_unit_map', {}).get(groupid, {})
			report_dict['group_legend_unit_map'] = legend_unit_map
			self.interpolateOrExtrapolateValues(report_data_dict, report_dict, type=0)
		# Do the pattern for the history datas.
		elif report_dict.get('report_type') == 'current_history':
			self.interpolateOrExtrapolateValues(report_data_dict, report_dict)
			history_data_dict = report_dict.get('history_data_dict', {}).get(groupid, {})
			legend_unit_map = report_dict.get('legend_unit_map', {}).get(groupid, {})
			self.getHistoryPattern(history_data_dict, legend_unit_map, report_dict)

	def getPatternData(self, report_data_dict, report_dict):
		"""Get the pattern data for the given pattern type
		Pattern Type: Hour of Day. Day of Week. Day of Month & Month of Year.
		"""
		st_time = time.time()
		pattern_type = safe_int(report_dict.get('pattern_type', 1))
		# Hour of day
		if pattern_type == 1:
			time_format = '%H:00'
			time_stamps = map(lambda a: iif(a < 10, '0%s:00' %(a), '%s:00' %(a)), range(24))
		# Day of Week
		elif pattern_type == 2:
			time_format = '%a'
			time_stamps = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
			time_stamps = map(lambda a: ChangeName(a), time_stamps)
		# Day of Month
		elif pattern_type == 3:
			time_format = '%d'
			time_stamps = map(lambda a: iif(a < 10, '0%s' %(a), '%s' %(a)), range(1, 32))
		# Month of Year
		else:# elif pattern_type == 4:
			time_format = '%b'
			time_stamps = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
			time_stamps = map(lambda a: ChangeName(a), time_stamps)
		self.pattern(time_stamps, time_format, report_data_dict)
		report_dict['time_stamps'] = time_stamps
		et_time = time.time()
		logMsg(2, 'Time taken to get Pattern Data - %s' %(`et_time - st_time`), self.module_name)

	def pattern(self, time_stamps, time_format, report_data_dict, history=0):
		"""Pattern the datas based on the time format and time_stamps.
		"""
		# Form legend_time_dict to get the patterned timestamp.
		legend_time_dict = {}
		for legend, datas in report_data_dict.items():
			if not legend_time_dict.has_key(legend):
				legend_time_dict[legend] = {}
			for time_stamp, value in datas:
				if time_format != '':
					formatted_time_stamp = time.strftime(time_format, time.localtime(time_stamp))
				else:
					formatted_time_stamp = time_stamp
				if not legend_time_dict[legend].has_key(formatted_time_stamp):
					legend_time_dict[legend][formatted_time_stamp] = []
				legend_time_dict[legend][formatted_time_stamp].append(value)
		# Calculate the average value for that time.
		for legend, datas in legend_time_dict.items():
			report_data_dict[legend] = {}
			for time_stamp, data in datas.items():
				report_data_dict[legend][time_stamp] = avg(data)
		# Add null values, if time_stamp is not present in the data dict.
		for legend, datas in report_data_dict.items():
			new_datas = []
			for time_stamp in time_stamps:
				if not datas.has_key(time_stamp):
					if not history:
						new_datas.append((time_stamp, 0.0))
					else:
						new_datas.append((time_stamp, NoValue))
				else:
					new_datas.append((time_stamp, datas.get(time_stamp)))
			report_data_dict[legend] = new_datas

	def interpolateOrExtrapolateValues(self, report_data_dict, report_dict, type=1):
		"""Interpolate / ExtraPolate the values based on the type
		If type = 1 means, Interpolate the values for the stacked charts to get the graph 
			similar to the trending graph.
		else, Extrapolate the values for the Current Vs Future graph.
		"""
		st_time = time.time()
		start_time, end_time, time_gap = self.getTimeRange(report_dict)
		if type:
			for legend, datas in report_data_dict.items():
				report_data_dict[legend] = self.interpolator.interpolate(datas, start_time, end_time, time_gap)
		else:
			legend_unit_map = report_dict.get('group_legend_unit_map', {})
			future_report_data_dict = {}
			for legend, datas in report_data_dict.items():
				new_datas = self.extrapolator.extrapolate(datas, start_time, end_time, time_gap)
				# Format the data based on the unit, if needed
				if legend_unit_map.get(legend) in self.units_to_be_formatted:
					new_formatted_datas = self.formatValue(new_datas, report_dict['division_factor'])
				else:
					new_formatted_datas = self.formatValue(new_datas)
				future_report_data_dict[legend] = new_formatted_datas
			report_dict['time_stamps'] = (start_time, end_time, time_gap)
			report_dict['trend_data'] = future_report_data_dict
		et_time = time.time()
		logMsg(2, 'Time taken to interpolate / extrapolate the values - %s' %(`et_time - st_time`), self.module_name)

	def formatValue(self, datas, division_factor=1):
		"""Format the values based on the division factor & chart director null value.
		"""
		new_datas = []
		for time_stamp, data in datas:
			if data != NoValue:
				new_datas.append((time_stamp, '%.2f' %(data / division_factor)))
			else:
				new_datas.append((time_stamp, data))
		return new_datas

	def getHistoryPattern(self, report_data_dict, legend_unit_map, report_dict):
		"""Get the history pattern data.
		"""
		st_time = time.time()
		start_time, end_time, time_gap = self.getTimeRange(report_dict)
		if maxInputStats.ENABLE_C3JS_GRAPH:
			time_format = ''
		else :
			time_format = report_dict.get('time_format')
		time_stamps = self.generateTime(start_time, end_time, time_gap,time_format)

		self.pattern(time_stamps, time_format, report_data_dict)
		report_dict['time_stamps'] = (start_time, end_time, time_gap)
		history_report_data_dict = {}
		for legend, datas in report_data_dict.items():
			if legend_unit_map.get(legend) in self.units_to_be_formatted:
				new_formatted_datas = self.formatValue(datas, report_dict['division_factor'])
			else:
				new_formatted_datas = self.formatValue(datas)
			history_report_data_dict[legend] = new_formatted_datas
		report_dict['time_stamps'] = (start_time, end_time, time_gap)
		report_dict['trend_data'] = history_report_data_dict
		et_time = time.time()
		logMsg(2, 'Time taken to get History Pattern - %s' %(`et_time - st_time`), self.module_name)

	def generateTime(self, start_time, end_time, time_gap, time_format):
		"""Generate the time based on the time gap from start_time and end_time.
		"""
		time_stamps = []
		cur_time = start_time
		while cur_time < end_time:
			if time_format !='':
				time_stamps.append(time.strftime(time_format, time.localtime(cur_time)))
			else:
				time_stamps.append(cur_time)
			cur_time += time_gap
		return time_stamps

	def getDataParams(self, groupid, report_data_dict, legend_unit_map, report_dict):
		"""Get the datas which are required for the report.
		Get the timestamps (x-axis labels) from the sub_report_data_dict
		and get the (legend, data_set) for the datas from the sub_report_data_dict
		"""
		report_type = report_dict.get('report_type')
		ip_data = []
		units = []
		time_stamps = []
		all_time_stamps = []
		new_legend_unit_map = {}
		legend_summary_data = {}
		for legend, unit in legend_unit_map.items():
			legend_datas = report_data_dict.get(legend, [])
			if unit in self.units_to_be_formatted:
				temp_division_factor = report_dict.get('division_factor')
				# reset the division factor for 'sec' unit.
				if unit == 'sec':
					temp_division_factor = 1
				new_datas = self.formatValue(legend_datas, temp_division_factor)
				
				if unit == 'bps':
					temp_unit = report_dict['unit_prefix'] + unit
				# for **bytes which is the mulitples of 1000
				elif unit[:2] == '**':
					temp_unit = report_dict['unit_prefix'] + unit[2:]
				# for *bytes which is the multiples of 1024
				elif unit[:1] == '*':
					temp_unit = report_dict['unit_prefix'] + unit[1:]
				else:
					temp_unit = unit
			else:
				new_datas = self.formatValue(legend_datas)
				temp_unit = unit
			units.append(temp_unit)
			new_legend_unit_map[legend] = temp_unit
			actual_datas = map(lambda a: safe_float(a), filter(lambda a, b = NoValue: a != b, map(lambda a: a[1], new_datas)))
			actual_datas.sort()
			timestamps = map(lambda a: a[0], new_datas)
			if len(time_stamps) < len(timestamps):
				time_stamps += timestamps
				time_stamps = unique(time_stamps)
				time_stamps.sort()
			all_time_stamps += timestamps
			# For Summary Values
			try:
				min_val = '%.2f' %(min(actual_datas))
				max_val = '%.2f' %(max(actual_datas))
				avg_val = '%.2f' %(avg(actual_datas))
				percentile_95 = '%.2f' %numpy.percentile(actual_datas, 95)
			except:
				min_val = '-'
				max_val = '-'
				avg_val = '-'
				percentile_95 = '-'
			if not legend_summary_data.has_key(legend):
				legend_summary_data[legend] = {}
			min_val_time, max_val_time = self.getMinMaxValTime(new_datas, min_val, max_val)
			legend_summary_data[legend]['min'] = min_val
			legend_summary_data[legend]['max'] = max_val
			legend_summary_data[legend]['avg'] = avg_val
			legend_summary_data[legend]['min_time'] = min_val_time
			legend_summary_data[legend]['max_time'] = max_val_time
			legend_summary_data[legend]['percentile_95'] = percentile_95
			legend_summary_data[legend]['_path'] = legend
			#try:
			#	legend_str = interText.interString['S5215']
			#except:
			#	legend_str = '%s Average : %s %s Maximum : %s %s Minimum : %s %s'
			if report_type in self.pie_charts:
				ip_data.append((safe_float(avg_val), (legend, avg_val)))

			elif report_type in self.data_table_charts:
				# Not Changing the Legend for Data Table Report
				new_datas.sort(sortTuple1)
				ip_data.append((safe_float(avg_val), (legend, new_datas)))
			elif report_type == 'summary':
				new_datas = (min_val, max_val, avg_val, percentile_95)
				ip_data.append((safe_float(avg_val), (legend, new_datas)))
			elif report_type == 'pattern':
				#temp_legend = legend_str %(legend, avg_val, temp_unit, max_val, temp_unit, min_val, temp_unit)
				temp_legend = legend
				ip_data.append((safe_float(avg_val), (temp_legend, new_datas)))
			else:
				new_datas.sort(sortTuple1)
				#temp_legend = legend_str %(legend, avg_val, temp_unit, max_val, temp_unit, min_val, temp_unit)
				temp_legend = legend
				ip_data.append((safe_float(avg_val), (temp_legend, new_datas)))
		# Dont sort the data for the resource split reports. to maintain the legend is same for the entire group.
		if report_dict.get('group_type') != 1:
			# sort the datas based on avg_val
			ip_data.sort(sortTuple1)
			# reverse the datas only if it is positive polarity
			if safe_int(report_dict.get('polarity')) > 0:
				ip_data.reverse()
		# remove the avg value which is added for sorting.
		report_data = map(lambda a: a[1], ip_data)
		report_dict['out']['unit'] = string.join(distinct(units), ', ')
		report_dict['out']['data'] = report_data
		report_dict['out']['units'] = units
		report_dict['out']['percentile_95'] = percentile_95
		# For AdHoc Report Link from Summary Table
		report_dict['out']['legend_report_link_map'] = report_dict.get('legend_report_link_map', {}).get(groupid, {})
		report_dict['legend_summary_data'] = legend_summary_data
		self.getDisplayTimePattern(report_dict)
		time_format = report_dict.get('time_format')
		report_dict['out']['data_time_format'] = time_format
		# more than one month is selected and resolution is not month
		if report_type not in self.pie_charts and report_type not in ['pattern', 'percentile'] and not self.table_types_map.has_key(report_type) and report_dict.get('diff_time') > (THIRTY_ONE_DAYS * ONE_DAY) and report_dict.get('resolution') != 'month':
			report_dict['month_graph'] = 1
			yr_mon = unique(map(lambda a: (time.localtime(safe_int(a))[0], time.localtime(safe_int(a))[1]), time_stamps))
			yr_mon_days = {}
			for year, month in yr_mon:
				no_of_days = getReportParams(report_dict).monthToDayDict.get(month, 31)
				if year % 4 == 0 and month == 2 :
					# Increasing Feb Days to 29 for Leap Yrs
					no_of_days = no_of_days + 1
				yr_mon_days[(year, month)] = no_of_days
			new_time_stamps = []
			null_value_positions = []
			for time_stamp in time_stamps:
				tuple_time = time.localtime(time_stamp)
				year = tuple_time[0]
				month = tuple_time[1]
				day = tuple_time[2]
				hour = tuple_time[3]
				minute = tuple_time[4]
				if yr_mon_days.get((year, month)) == day:
					new_time_stamps.append(time.strftime(time_format, tuple_time))
					if time_format == '%d/%m/%y':
						while day < THIRTY_ONE_DAYS:
							# break the loop if it crosses 31 days.
							dummy_date = '%d/%d/%d' %(day + 1, month, year)
							# add this dummy date only if not exists.
							if dummy_date not in new_time_stamps:
								null_value_positions.append((len(new_time_stamps), dummy_date))
								new_time_stamps.append(dummy_date)
							# do for every day
							day = day + 1
					elif time_format == '%d/%m %H:00' or time_format == '%d/%m/%y %H:00':
						while day < THIRTY_ONE_DAYS:
							# break the loop if it crosses 31 days.
							while hour < TWENTY_FOUR_HOURS:
								# break the loop if it crosses 24 hours.
								if time_format == '%d/%m %H:00':
									dummy_date = '%d/%d %d:00' %(day + 1, month, hour)
								else:
									dummy_date = '%d/%d/%d %d:00' %(day + 1, month, year, hour)
								# add this dummy date only if not exists.
								if dummy_date not in new_time_stamps:
									null_value_positions.append((len(new_time_stamps), dummy_date))
									new_time_stamps.append(dummy_date)
								# do for every hour
								hour = hour + 1
							# do for every day
							day = day + 1
							# reset the hour as day is changed
							hour = 0
					elif time_format == '%m/%d':
						while day < THIRTY_ONE_DAYS:
							# break the loop if it crosses 31 days.
							dummy_date = '%d/%d' %(day + 1, month)
							# add this dummy date only if not exists.
							if dummy_date not in new_time_stamps:
								null_value_positions.append((len(new_time_stamps), dummy_date))
								new_time_stamps.append(dummy_date)
							# do for every day
							day = day + 1
					elif time_format == '%m/%d %H:00' or time_format == '%m/%d/%y %H:00':
						while day < THIRTY_ONE_DAYS:
							# break the loop if it crosses 31 days.
							while hour < TWENTY_FOUR_HOURS:
								# break the loop if it crosses 24 hours.
								if time_format == '%m/%d %H:00':
									dummy_date = '%d/%d %d:00' %(day + 1, month, hour)
								else:
									dummy_date = '%d/%d/%d %d:00' %(day + 1, month, year, hour)
								# add this dummy date only if not exists.
								if dummy_date not in new_time_stamps:
									null_value_positions.append((len(new_time_stamps), dummy_date))
									new_time_stamps.append(dummy_date)
								# do for every hour
								hour = hour + 1
							# do for every day
							day = day + 1
							# reset the hour as day is changed
							hour = 0
					# currently more than two months and minute resolution is not supported
					# as it has performance issue of constructing graph.
					#elif time_format == '%d/%m %H:%M' or time_format == '%d/%m/%y %H:%M':
					#	while day < THIRTY_ONE_DAYS:
					#		# break the loop if it crosses 31 days.
					#		while hour < TWENTY_FOUR_HOURS:
					#			# break the loop if it crosses 24 hours.
					#			while minute < SIXTY_MINUTES:
					#				# break the loop if it crosses 60 minutes.
					#				if time_format == '%d/%m %H:%M':
					#					dummy_date = '%d/%d %d:%d' %(day + 1, month, hour, minute)
					#				else:
					#					dummy_date = '%d/%d/%d %d:%d' %(day + 1, month, year, hour, minute)
					#				# add this dummy date only if not exists.
					#				if dummy_date not in new_time_stamps:
					#					null_value_positions.append((len(new_time_stamps), dummy_date))
					#					new_time_stamps.append(dummy_date)
					#				# do for every minute
					#				minute = minute + 1
					#			# do for every hour
					#			hour = hour + 1
					#			# reset the minute as hour is crossed
					#			minute = 0
					#		# do for every day
					#		day = day + 1
					#		# reset the hour as day is changed.
					#		hour = 0
				else:
					new_time_stamps.append(time.strftime(time_format, tuple_time))
			data = report_dict['out']['data']
			for legend, datas in data:
				for pos, dummy_date in null_value_positions:
					datas.insert(pos, (dummy_date, NULL_VALUE))
			report_dict['out']['time_stamps'] = new_time_stamps
			report_dict['out']['data'] = data
		else:
			# For other than stacked reports.
			if report_type not in self.stacked_charts:
				# For current Vs history, future, pattern reports
				if report_dict.get('time_stamps'):
					report_dict['out']['time_stamps'] = report_dict['time_stamps']
					#report_dict['out']['time_format'] = time_format
				# For Trending time
				else:
					report_dict['out']['time_stamps'] = self.getTimeRange(report_dict)
					if report_dict['resolution'] == 'month' :
						report_dict['month_graph'] = 1
						new_time_stamps = map(lambda a: time.strftime(time_format, time.localtime(safe_int(a))), time_stamps)
						report_dict['out']['time_stamps'] = new_time_stamps
			# For stacked reports
			else:
				new_time_stamps = map(lambda a: time.strftime(time_format, time.localtime(safe_int(a))), time_stamps)
				report_dict['out']['time_stamps'] = new_time_stamps
		if report_dict.has_key('label_step_factor'):
			report_dict['out']['label_step_factor'] = report_dict['label_step_factor']
		report_dict['out']['data_time_stamps'] = distinct(all_time_stamps)
		report_dict['out']['legend_unit_map'] = new_legend_unit_map
		report_dict['out']['width'] = report_dict.get('width', DEFAULT_REPORT_WIDTH)

	def getMinMaxValTime(self, datas, min_val, max_val):
		min_time = 0
		max_time = 0
		datas.sort(sortTuple1)
		for timeStamp, data in datas:
			if data != NoValue and safe_float(data) == safe_float(min_val):
				min_time = timeStamp
				break
		for timeStamp, data in datas:
			if data != NoValue and safe_float(data) == safe_float(max_val):
				max_time = timeStamp
				break
		return min_time, max_time

	def getDisplayTimePattern(self, report_dict):
		"""Get the display time pattern based on the time scale
		as well as the resolution being selected.
		"""
		init_option = safe_int(report_dict['request']['init_option'])
		resolution = report_dict.get('resolution', '')
		time_format = getReportParams(report_dict).chart_time_format_map.get(getReportParams(report_dict).time_format_map.get(resolution))
		report_dict['time_format'] = getReportParams(report_dict).time_format_map.get(resolution)
		if maxInputStats.isSimulation == 1:
			time_scale = 2
			time_scale_option = getReportParams(report_dict).time_scale.get(time_scale)
			time_format = getReportParams(report_dict).chart_time_format_map.get(getReportParams(report_dict).time_format_map.get(resolution))
			report_dict['time_format'] = getReportParams(report_dict).time_format_map.get(resolution)
			report_dict['out']['time_format'] = time_format
			return
		flag = 0
		# if default duration is selected
		if init_option == 1:
			time_scale = safe_int(report_dict['request']['timescale'])
			time_scale_option = getReportParams(report_dict).time_scale.get(time_scale)
			# if default resolution is selected for that time scale
			if resolution == time_scale_option[2]:
				resolution_time_format = getReportParams(report_dict).time_format_map.get(resolution)
				if report_dict.get('realtime'):
					resolution_time_format = '%H:%M:%S'
				time_format = getReportParams(report_dict).chart_time_format_map.get(resolution_time_format)
				report_dict['time_format'] = resolution_time_format
			# if different (lower/higher) resolution is selected
			else:
				default_time_format = time_scale_option[4]
				default_resolution = time_scale_option[2]
				default_higher_resolution = time_scale_option[3]
				resolution_time_format = getReportParams(report_dict).time_format_map.get(resolution)
				# default higher resolution is being selected.
				# for other higher resolution, we may not get the data, 
				# so ne need to bother about that.
				if default_higher_resolution == resolution:
					time_format = getReportParams(report_dict).chart_time_format_map.get(resolution_time_format)
					report_dict['time_format'] = resolution_time_format
				# suppose, lower resolution selected
				else:
					if default_time_format == resolution_time_format:
						time_format = getReportParams(report_dict).chart_time_format_map.get(resolution_time_format)
						report_dict['time_format'] = resolution_time_format
					else:
						if default_resolution == "year" : # Year
							if resolution == "month" : # Selected Day
								time_format = 'mmm yy'
								report_dict['time_format'] = "%b'%y"
							elif resolution == "day" : # Selected Day
								#time_format = 'dd/mm/yy'
								#report_dict['time_format'] = '%d/%m/%y'
								time_format = 'yyyy/mm/dd'
								report_dict['time_format'] = '%Y/%m/%d'
						if default_resolution == "month" : # Month
							if resolution == "day" : # Selected Day
								time_format = 'mm/dd/yy'
								report_dict['time_format'] = '%d/%m/%y'
							elif resolution == "hour" :
								time_format = 'mm/dd/yy hh:00'
								report_dict['time_format'] = '%m/%d %H:00'
								flag = 1
							elif resolution == "raw" :
								time_format = 'mm/dd/yy hh:nn'
								report_dict['time_format'] = '%m/%d/%y %H:%M'
								flag = 1
						else :
							time_format = getReportParams(report_dict).chart_time_format_map.get(default_time_format) + ' ' + getReportParams(report_dict).chart_time_format_map.get(resolution_time_format)
							report_dict['time_format'] = default_time_format + ' ' + resolution_time_format
							flag = 1
		# if calendar is selected.
		elif init_option == 2:
			selected_time = report_dict['request'].get('cal_val', '')
			# if date is selected
			if selected_time.find('-') != -1:
				default_resolution = 'hour'
			# if week is selected
			elif selected_time.find('$$') != -1:
				default_resolution = 'day'
			# if month is selected
			else: # selected_time.find(', ') != -1:	
				default_resolution = 'day'
			default_time_format = getReportParams(report_dict).time_format_map.get(default_resolution)
			resolution_time_format = getReportParams(report_dict).time_format_map.get(resolution)
			# if suppose, same resolution is being selected
			if default_time_format == resolution_time_format:
				time_format = getReportParams(report_dict).chart_time_format_map.get(resolution_time_format)
				report_dict['time_format'] = resolution_time_format
			# if different (lower/higher) resolution is selected
			else:
				if default_resolution == 'hour' and resolution == 'raw':
					time_format = getReportParams(report_dict).chart_time_format_map.get(resolution_time_format)
					report_dict['time_format'] = resolution_time_format
				else:
					time_format = getReportParams(report_dict).chart_time_format_map.get(default_time_format) + ' ' + getReportParams(report_dict).chart_time_format_map.get(resolution_time_format)
					report_dict['time_format'] = default_time_format + ' ' + resolution_time_format
					flag = 1
		# if custom time is selected.
		else:
			format, time_format, flag = self.getResolutionTimeFormat(report_dict['st'], report_dict['et'], resolution)
			report_dict['time_format'] = format
		report_dict['out']['time_format'] = time_format
		if flag:
			report_dict['out']['x_label_style'] = {'angle': 15}

	def getResolutionTimeFormat(self, st_time, et_time, resolution):
		"""Construct the Time Format
		"""
		flag = 0
		# less than or equal to two days
		if resolution == 'raw':
			format = '%H:%M'
			# more than one day
			# Either Date are not same or start_time - end_time > ONE_DAY
			if (time.localtime(st_time)[2] != time.localtime(et_time)[2]) or (safe_int(et_time) - safe_int(st_time)) > ONE_DAY :
				format = "%m/%d/%Y %H:%M"
				flag = 1
		elif resolution == 'hour':
			format = '%H:00'
			# Either Date are not same or start_time - end_time > ONE_DAY
			if (time.localtime(st_time)[2] != time.localtime(et_time)[2]) or (safe_int(et_time) - safe_int(st_time)) > ONE_DAY :
				#format = '%d/%m %H:00'
				#Added by sathish to display result time format
				format = "%m/%d/%Y %H:00:00"
				#end here
				
				flag = 1
		elif resolution == 'day':
			#format = '%d/%m'
			format = "%m/%d/%Y"
			
		else:#elif resolution == 'month':
			format = '%b %y'
		# Bug Id : 497
		# adding the default format type if it is not there in the reportparams.py.
		# So better put any format missing in the reportparams.py file
		chart_format = getReportParams({}).chart_time_format_map.get(format, 'mm/dd/yyyy hh:nn')
		return format, chart_format, flag

	def drawGraph(self, report_dict):
		"""Draw the graph based on the report type.
		"""
		report_type = report_dict.get('report_type', 'trend')
		if not self.table_types_map.has_key(report_type):
			params = self.getChartParams(report_type, report_dict)
			report_dict['params'] = params
			if report_dict.get('jsgraphs',''):
				return params
			method = getReportParams(report_dict).report_class_map.get(report_type)
			# Calling Different Class for the Trend Type Month Resolution
			if method == TrendChart and safe_int(report_dict.get('month_graph')):
				method = TrendMonthChart
			if (report_dict.has_key("overview") and safe_int(report_dict.get("overview"))==1 and report_dict.has_key("c3js") and safe_int(report_dict.get("c3js"))==1 and report_type not in self.NON_C3_REPORT_TYPES) or (safe_int(report_dict.get("overview"))==0 and report_type not in self.NON_C3_REPORT_TYPES) and not report_dict.get('image_only'):
				# topn = safe_int(report_dict.get('topN'))
				# if topn > 10:
				# 	report_dict['topN']=10
				c3_report_type = report_dict.get('report_type','line')
				if report_dict.has_key('c3_report_type'):
					c3_report_type = report_dict.get('c3_report_type')
				if c3_report_type == 'trend':
					c3_report_type = 'line'
				each_content = params
				js_param={}
				labels = []
				mrtgobjs={}
				if c3_report_type == 'line_bar':
					c3data_time_map,c3data,graph_reporttype_map,color_map= self.formatC3RangeData(each_content.get('data'))
					js_param['graph_reporttype_map'] = graph_reporttype_map
					js_param['color_map'] = color_map
					c3_report_type = 'bar'
				elif c3_report_type.find('mrtg')!=-1:
					c3data_time_map,c3data,graph_reporttype_map,color_map,mrtgdata,name,mrtgobjs= self.formatMRTGRangeData(each_content.get('data'))
					js_param['graph_reporttype_map'] = graph_reporttype_map
					js_param['color_map'] = color_map
					js_param['mrtgdata'] = mrtgdata
					js_param['name']=name
					js_param['chart_legends']=report_dict.get('legend_summary_data', {}).keys()					
				elif c3_report_type == 'pie':
					c3data, labels = self.formatC3DataPie(each_content.get('data'))
				elif c3_report_type in ['current_history','current_future']:
					c3data_time_map,c3data,graph_reporttype_map,color_map = self.formatC3DataCurVsHis(each_content,c3_report_type)
					js_param['graph_reporttype_map'] =  graph_reporttype_map
					js_param['color_map'] = color_map
					c3_report_type = 'spline'
				else:
					c3data_time_map,c3data= self.formatC3Data(each_content.get('data'))
				x_axis_labels = each_content.get('labels',{}).get('x_axis_labels',[])
				x_label_step = safe_int(each_content.get('labels',{}).get('x_label_step'))
				timeZoneDifference = report_dict.get('timeZoneDifference',0)
				start_time = (report_dict.get('st') + timeZoneDifference)*1000
				end_time = (report_dict.get('et') + timeZoneDifference)*1000
				if safe_int(report_dict.get('timescale')) != 18 or (c3_report_type.find('mrtg')==-1 and safe_int(report_dict.get('timescale')) == 18):
					if len(x_axis_labels) >= 3:
						time_gap = x_axis_labels[2]*1000
				unit = each_content.get('unit')
				title = each_content['chart_title'].get('title')
				x_label_format = string.strip(each_content.get('labels',{}).get('x_label_format'))
				time_format = getReportParams({}).chart_time_format_reverse_map.get(x_label_format)
				if safe_int(report_dict.get('month_graph')):
					time_gap = 86400 * 1000
					#if report_dict.get('resolution') == 'month':
					#	time_format = '%m/%y'
					#else:
					time_format = '%d/%m/%y'
					x_label_step = 31
					if (report_dict.get('et') - report_dict.get('st')) > (86400*366):
						x_label_step = 31*3
				if c3_report_type.find('mrtg')!=-1:
					if safe_int(report_dict.get('timescale')) in [7]:
						time_format = '%H:%M'
					if safe_int(report_dict.get('timescale')) in [10]:
						time_format = '%a'
						#time_gap=1800*1000
					if safe_int(report_dict.get('timescale')) in [13]:
						time_format = '%U'
						if report_dict.get('resolution') == 'raw':
							x_label_step=60*24*7
						if report_dict.get('resolution') == 'hour':	
							x_label_step=24*7
						if report_dict.get('resolution') == 'day':	
							x_label_step=3.5						
						#time_gap=7200*1000
					if safe_int(report_dict.get('timescale')) in [18]:
						time_format = '%b'
						time_gap = 86400 * 1000
				if c3_report_type == 'percentile':
					value_95_percent = report_dict['out'].get('percentile_95',0)
					if value_95_percent:
						js_param['95th_percentile'] = value_95_percent
					c3_report_type = 'line'
				js_param['c3data'] = c3data
				js_param['labels'] = labels
				js_param['unit'] = unit
				js_param['title'] = title
				js_param['report_type'] = c3_report_type
				js_param['width'] = report_dict.get('width')
				js_param['report_link'] = ""
				js_param['report_timezone'] = time.timezone
				if safe_int(report_dict.get("overview"))==1:
					js_param['overview_graph'] = 1
					js_param['width'] = 480
					
				if c3_report_type != 'pie':
					js_param['c3data_time_map'] = c3data_time_map
					js_param['start_time'] = start_time
					js_param['end_time'] = end_time
					js_param['time_gap'] = time_gap
					js_param['x_label_step'] = x_label_step
					js_param['time_format'] = time_format
					js_param['timescale'] = safe_int(report_dict.get('timescale',8))
					red_val = safe_float(report_dict.get('report_obj', {}).get('red_val', ''))
					yellow_val = safe_float(report_dict.get('report_obj', {}).get('yellow_val', ''))
					js_param['red_val'] = red_val
					js_param['yellow_val'] = yellow_val

				title_chart = title
				if title_chart.lower().find('\r\n/')!=-1:
					title_chart = title_chart.replace('\r\n/','_')
				if title_chart.lower().find('\r\n')!=-1:
					title_chart = title_chart.replace('\r\n/',' ')
					title_chart = title_chart.strip()
				if title_chart.lower().find(' ')!=-1:
					title_chart = title_chart.replace(' ','_')
				if title_chart.lower().find('/')!=-1:
					title_chart = title_chart.replace('/','_')
				if title_chart.lower().find('.')!=-1:
					title_chart = title_chart.replace('.','_')
				c3_chart_count = report_dict['c3_chart_count']				
				#chartid = 'chart_%s_%s'%(title_chart.strip(),c3_chart_count)
				js_param['chart_title'] = title_chart
				chartid = getUniqueString()
				logMsg(4, "Calling Draw Graph ahain %s"%chartid, self.module_name)
				C3_JS_str = ""
				if c3_report_type.find('mrtg')!=-1:
					report_dict['C3_JS'].extend([XMLCSSLib('C3/css/c3.min.css'), XMLJSLib('C3/js/d3.min.js'), XMLJSLib('C3/js/c3.min.js'), XMLJSLib('C3/C3JSChart.js')])
				else:
					C3_JS_str = XMLCSSLib('C3/css/c3.min.css').toHTML({}) + XMLJSLib('C3/js/d3.min.js').toHTML({}) + XMLJSLib('C3/js/c3.min.js').toHTML({}) + XMLJSLib('C3/C3JSChart.js').toHTML({})
				if c3_report_type == 'pie':
					# ret = [XMLDivTagObj(chartid), XMLDivTagObj(chartid + '_legend_container')]
					ret = ['<div class="c3-container"><div id="%s"></div><div class="%s_legend_container"></div></div>' % (chartid, chartid)]
					#report_dict['C3_JS'].append(XMLJS("drawC3JSChartPie('%s',%s)")%(chartid,json.dumps(js_param)))
					C3_JS_str += XMLJS("drawC3JSChartPie('%s',%s)")%(chartid,json.dumps(js_param))
					report_dict['C3_JS'].append(C3_JS_str)
				elif c3_report_type.find('mrtg')!=-1:
					ret = ['<div class="c3-container"><div id="%s"></div><div class="%s_legend_container" style="height:20;"></div></div>' % (chartid,chartid)]
					try:
						if report_dict.get('legend_summary_data', {}).keys() in [['Throughput In', 'Throughput Out'],['Throughput Out', 'Throughput In']]:
							legend_info='<div><table class="mrtg_div_table"><tr><td></td><td><b>Max</b></td><td><b>Avg</b></td><td><b>Current<b></td></tr><tr><td style="color:#00cc00"><b>In</b></td><td>%.1f %s</td><td>%.1f %s</td><td>%.1f %s</td></tr><tr><td style="color:#0000ff"><b>Out</b></td><td>%.1f %s</td><td>%.1f %s</td><td>%.1f %s</td></tr></table></div>' % (mrtgobjs.get('Throughput In_max',''),unit,mrtgobjs.get('Throughput In_avg',''),unit,mrtgobjs.get('Throughput In_current',''),unit,mrtgobjs.get('Throughput Out_max',''),unit,mrtgobjs.get('Throughput Out_avg',''),unit,mrtgobjs.get('Throughput Out_current',''),unit)
							ret[0]+= legend_info
						elif report_dict.get('legend_summary_data', {}).keys() in [['Throughput In']]:
							legend_info='<div><table class="mrtg_div_table"><tr><td></td><td><b>Max</b></td><td><b>Avg</b></td><td><b>Current<b></td></tr><tr><td style="color:#00cc00"><b>In</b></td><td>%.1f %s</td><td>%.1f %s</td><td>%.1f %s</td></tr></table></div>' % (mrtgobjs.get('Throughput In_max',''),unit,mrtgobjs.get('Throughput In_avg',''),unit,mrtgobjs.get('Throughput In_current',''),unit)
							ret[0]+= legend_info
						elif report_dict.get('legend_summary_data', {}).keys() in [['Throughput Out']]:
							legend_info='<div><table class="mrtg_div_table"><tr><td></td><td><b>Max</b></td><td><b>Avg</b></td><td><b>Current<b></td></tr><tr><td style="color:#0000ff"><b>Out</b></td><td>%.1f %s</td><td>%.1f %s</td><td>%.1f %s</td></tr></table></div>' % (mrtgobjs.get('Throughput Out_max',''),unit,mrtgobjs.get('Throughput Out_avg',''),unit,mrtgobjs.get('Throughput Out_current',''),unit)
							ret[0]+= legend_info
					except:
						pass
					# ret = [XMLDivTagObj(chartid), XMLDivTagObj(chartid + '_legend_container')]
					# chartid = 'chart'
					# ret = [XMLDivTagObj('c3-container',[XMLDivTagObj(chartid), XMLDivTagObj('container')])]					
					report_dict['C3_JS'].append(XMLJS("drawMRTGJSChart('%s',%s)")%(chartid,json.dumps(js_param)))
				else:
					# ret = [XMLDivTagObj(chartid), XMLDivTagObj(chartid + '_legend_container')]
					# chartid = 'chart'
					# ret = [XMLDivTagObj('c3-container',[XMLDivTagObj(chartid), XMLDivTagObj('container')])]
					ret = ['<div class="c3-container"><div id="%s"></div><div class="%s_legend_container"></div></div>' % (chartid, chartid)]
					#report_dict['C3_JS'].append(XMLJS("drawC3JSChart('%s',%s)")%(chartid,json.dumps(js_param)))
					C3_JS_str += XMLJS("drawC3JSChart('%s',%s)")%(chartid,json.dumps(js_param))
					report_dict['C3_JS'].append(C3_JS_str)
			else:
				instance = method(params)
				chart_name = "report" + os.sep + instance.chartURL
				image_map = instance.imageMap
				image_map = string.replace(image_map, "<BR>", "")
				image_map = string.replace(image_map, "\n", "")
				ret = XMLImage(chart_name, useMap=chart_name, imageMap=image_map, onmouseout='javascript:hideMouseOver();')
			# For HTML Offline Reports
			if safe_int(report_dict.get('html_offline_report')) and not maxInputStats.ENABLE_C3JS_GRAPH:
				if not report_dict.has_key('created_charts'):
					report_dict['created_charts'] = []
				report_dict['created_charts'].append(chart_name)
			elif safe_int(report_dict.get('html_offline_report')) and maxInputStats.ENABLE_C3JS_GRAPH:
				if not report_dict.has_key('created_charts'):
					report_dict['created_charts'] = []
				report_dict['created_charts'].append(ret)				

			#Uttam support for both XLS / CSV to support datatables for reports with Graphs !!
			if safe_int(report_dict.get('format_csv')) and safe_int(report_dict.get('format_csv')) in [1,2] and safe_int(report_dict.get('overview')) == 0:
				ret = self.makeDataTable(report_dict['out'], report_dict)
			#else:
				# method = getReportParams(report_dict).report_class_map.get(report_type)
				# # Calling Different Class for the Trend Type Month Resolution
				# if method == TrendChart and safe_int(report_dict.get('month_graph')):
				# 	method = TrendMonthChart
				# instance = method(params)
				# chart_name = "report" + os.sep + instance.chartURL
				# image_map = instance.imageMap
				# image_map = string.replace(image_map, "<BR>", "")
				# image_map = string.replace(image_map, "\n", "")
			#	ret = XMLImage(chart_name, useMap=chart_name, imageMap=image_map, onmouseout='javascript:hideMouseOver();')
			if report_dict.has_key('legend_summary_data') and safe_int(report_dict.get('overview')) == 0 and safe_int(report_dict.get('image_only')) == 0:
				if maxInputStats.SHOW_GRAPH_SUMMARY_TABLE and report_dict.get('width', DEFAULT_REPORT_WIDTH) != SMALL_REPORT_WIDTH:
					table = self.makeGraphSummaryTable(params, report_dict)
					ret = [ret, table]
		else:
			if report_dict.get('jsgraphs',''):
				report_dict['params']=report_dict.get('out',{})
				return report_dict.get('out',{}).get('data', [])
			ret = self.table_types_map[report_type](report_dict['out'], report_dict)
		return ret

	def formatC3Data(self,HSreportdata):
        	c3data_time_map = {}
        	c3data=[]
        	for data in HSreportdata:
			temp = []
			temp1 = []
                	stat_name = data[0]
        		stat_time_map = '%s_timestamp'%(stat_name)
        		temp=[stat_time_map]
        		temp1=[stat_name]
        		c3data_time_map[stat_name] = stat_time_map
        		for elem in data[1]:
				if type(elem[0]) == type('') or str(elem[1])== '1.7e+308':
					continue
				else:
					yValue=str(elem[1])
					time = elem[0]*1000
					value = str(yValue)
					temp.append(time)
					temp1.append(value)
			c3data.append(temp)
			c3data.append(temp1)
		return c3data_time_map,c3data

	def formatC3RangeData(self,HSreportdata):
        	c3data_time_map = {}
        	c3data=[]
        	graph_reporttype_map = {}
        	color_map = {}
        	for data in HSreportdata:
                	stat_name = data[0]
        		stat_time_map = '%s_timestamp'%(stat_name)
        		temp_min=[stat_time_map+'_min']
        		temp_max=[stat_time_map+'_max']
        		temp_avg= [stat_time_map+'_avg']
        		temp1_min=[stat_name+'_min']
        		temp1_max=[stat_name+'_max']
        		temp1_avg=[stat_name+'_avg']
        		c3data_time_map[stat_name+'_min'] = stat_time_map+'_min'
        		c3data_time_map[stat_name+'_max'] = stat_time_map+'_max'
        		c3data_time_map[stat_name+'_avg'] = stat_time_map+'_avg'
        		graph_reporttype_map[stat_name+'_avg'] = 'spline'
     			color_map[stat_name+'_min'] = 'rgb(215, 232, 248)'
        		color_map[stat_name+'_avg'] = 'orange'
        		counter = 0
        		for elem in data[1]:
				if str(elem[1])== '1.7e+308':
					yValue='null'
				else:
					yValue=str(elem[1])
				time = elem[0]*1000
				value = str(yValue)
				if counter == 0:
					temp_min.append(time)
					temp1_min.append(value)
				if counter == 1:
					temp_max.append(time)
					temp1_max.append(value)
				if counter == 2:
					temp_avg.append(time)
					temp1_avg.append(value)
				if counter == 2:
					counter = 0
				else:
					counter = counter + 1
			c3data.append(temp_max)
			c3data.append(temp1_max)
			c3data.append(temp_min)
			c3data.append(temp1_min)
			c3data.append(temp_avg)
			c3data.append(temp1_avg)
		return c3data_time_map,c3data,graph_reporttype_map,color_map

	def formatMRTGRangeData(self,HSreportdata):
		c3data_time_map = {}
        	c3data=[]
        	mrtgdata=[]
		mrtgobjs={}
        	mrtgtimestamp=[]
        	graph_reporttype_map = {'Throughput In':'area-spline','Throughput Out':'spline'}
        	color_map = {'Throughput In':'#00cc00','Throughput Out':'#0000ff'}
        	name={'Throughput In':'In','Throughput Out':'Out'}
        	for data in HSreportdata:
                	stat_name = data[0]
        		stat_time_map = '%s_timestamp'%(stat_name)
        		temp_min=[stat_time_map+'_min']
        		temp_max=[stat_time_map+'_max']
        		temp_avg= [stat_time_map+'_avg']
        		temp_current=['timestamp']
        		temp1_min=[stat_name+'_min']
        		temp1_max=[stat_name+'_max']
        		temp1_avg=[stat_name+'_avg']
        		temp1_current=[stat_name]
        		temp1_data=[]
        		c3data_time_map[stat_name+'_min'] = stat_time_map+'_min'
        		c3data_time_map[stat_name+'_max'] = stat_time_map+'_max'
        		c3data_time_map[stat_name+'_avg'] = stat_time_map+'_avg'
        		#graph_reporttype_map[stat_name] = 'area-spline'
     			#color_map[stat_name+'_min'] = 'rgb(215, 232, 248)'
        		#color_map[stat_name+'_avg'] = 'orange'
        		counter = 0
        		for elem in data[1]:
				if str(elem[1])== '1.7e+308':
					yValue='null'
				else:
					yValue=str(elem[1])
				time = elem[0]*1000
				value = str(yValue)
				if value == 'null':
                    			continue
				if counter == 0:
					temp_min.append(time)
					temp1_min.append(value)
				if counter == 1:
					temp_max.append(time)
					temp1_max.append(value)
				if counter in [0,1,2]:
					temp_avg.append(time)
					temp1_avg.append(value)
					temp_current.append(time)
					temp1_current.append(value)
					temp1_data.append(safe_float(value))
				if counter == 2:
					counter = 0
				else:
					counter = counter + 1
			c3data.append(temp_max)
			c3data.append(temp1_max)
			c3data.append(temp_min)
			c3data.append(temp1_min)
			c3data.append(temp_avg)
			c3data.append(temp1_avg)
			mrtgtimestamp.append(temp_current)			
			mrtgdata.append(temp1_current)
			if temp1_data:
				mrtgobjs[stat_name+'_max']=max(temp1_data)
				mrtgobjs[stat_name+'_avg']=sum(temp1_data)/safe_float(len(temp1_data))
				mrtgobjs[stat_name+'_current']=temp1_data[-1]
		mrtgdata.append(mrtgtimestamp[0])
		if mrtgdata[0][0]=='Throughput Out' and mrtgdata[1][0] == 'Throughput In':
		    mrtgdatatemp=mrtgdata[0]
		    mrtgdata[0]=mrtgdata[1]
		    mrtgdata[1]=mrtgdatatemp
		#print mrtgdata,mrtgobjs,temp1_data,temp_current
		return c3data_time_map,c3data,graph_reporttype_map,color_map,mrtgdata,name,mrtgobjs

	def formatC3DataPie(self,HSreportdata):
        	c3data=[]
		labels = []
        	for data in HSreportdata:
			temp = []
			temp1 = []
                	stat_name = data[0]
        		temp1=[data[0],data[1]]
			labels.append(stat_name)
			c3data.append(temp1)
		return c3data, labels

	def formatC3DataCurVsHis(self,HSreportdata,c3_report_type):
		datas = HSreportdata.get('data')
        	trend_datas = HSreportdata.get('trend_data')
        	#print "trend_datas =>",trend_datas

        	c3data_time_map = {}
        	c3data=[]
        	graph_reporttype_map = {}
        	color_map = {}
        	temp_current =[]
        	chart1_ext = ''

        	if c3_report_type == 'current_history':
        		chart1_ext = '_history'
        	elif c3_report_type == 'current_future':
        		chart1_ext = '_future'


		for data in trend_datas:
			stat_name = data[0]
			stat_time_map = '%s_timestamp'%(stat_name)
			temp_history=[stat_time_map+chart1_ext]
			temp1_history=[stat_name+chart1_ext]
			c3data_time_map[stat_name+chart1_ext] = stat_time_map+chart1_ext
			graph_reporttype_map[stat_name+chart1_ext] = 'area'
			color_map[stat_name+chart1_ext] = 'rgb(205,248,255)'

        		for elem in data[1]:
				if type(elem[0]) == type('') or str(elem[1])== '1.7e+308':
					continue
				else:
					yValue=str(elem[1])
					time = elem[0]*1000
					value = str(yValue)
					temp_history.append(time)
					temp1_history.append(value)
			c3data.append(temp_history)
			c3data.append(temp1_history)

        	for data in datas:
                	stat_name = data[0]
        		stat_time_map = '%s_timestamp'%(stat_name)
        		#temp_min=[stat_time_map+'_min']
        		#temp_avg= [stat_time_map+'_avg']
        		temp_max=[stat_time_map]
        		#temp1_min=[stat_name+'_min']
        		#temp1_avg=[stat_name+'_avg']
        		temp1_max=[stat_name]
        		#c3data_time_map[stat_name+'_min'] = stat_time_map+'_min'
        		#c3data_time_map[stat_name+'_avg'] = stat_time_map+'_avg'
        		c3data_time_map[stat_name] = stat_time_map
        		graph_reporttype_map[stat_name] = 'spline'
     			#color_map[stat_name+'_min'] = 'rgb(215, 232, 248)'
        		color_map[stat_name] = 'orange'
        		counter = 0
        		finishedele=[]
        		for elem in data[1]:
        			if elem[0] not in finishedele:
	        			vals = sorted([float(item[1]) for item in data[1] if item[0] == elem[0]])
					time = elem[0]*1000
					if len(vals) == 3:
						# temp_min.append(time)
						# temp1_min.append(vals[0] if  str(vals[0])!= '1.7e+308' else '0')
						# temp_avg.append(time)
						# temp1_avg.append(vals[1] if  str(vals[1])!= '1.7e+308' else '0')
						if type(vals[2]) == type('') or str(vals[2])== '1.7e+308' :
							continue
						else :
							temp_max.append(time)
							temp1_max.append(vals[2])
						
					elif len(vals) == 1:
						if type(vals[0]) == type('') or str(vals[0])== '1.7e+308' :
							continue
						else:
							temp_max.append(time)
							temp1_max.append(vals[0])
						
					finishedele.append(elem[0])
			
			# c3data.append(temp_min)
			# c3data.append(temp1_min)
			# c3data.append(temp_avg)
			# c3data.append(temp1_avg)
			c3data.append(temp_max)
			c3data.append(temp1_max)
		
		return c3data_time_map,c3data,graph_reporttype_map,color_map

	def getChartParams(self, report_type, report_dict):
		"""Get the chart params for the current report type
		"""
		out = report_dict.get('out', {})
		temp_params = copy.deepcopy(eval('getReportParams(report_dict).%s_params' %(report_type)))
		params = {}
		params.update(temp_params)
		# From Overview
		if report_dict.get('overview'):
			# For Small Graphs in Overview
			if not report_dict.get('small_graph'):
				temp_params = copy.deepcopy(eval('getReportParams(report_dict).overview_params'))
				params = {}
				params.update(temp_params)
				if hasattr(getReportParams(report_dict), 'overview_%s_params' %report_type):
					params = recursiveupdate(params, eval('getReportParams(report_dict).overview_%s_params' %(report_type)))
		params['labels']['x_axis_labels'] = out.get('time_stamps', [])
		params['labels']['x_label_format'] = out.get('time_format', '')
		params['labels']['x_label_style'] = out.get('x_label_style', {})
		if out.has_key('label_step_factor'):
			params['labels']['x_label_step'] = out.get('label_step_factor')
		params['data_count'] = len(out.get('data', []))
		params['data'] = out.get('data', [])
		params['unit'] = out.get('unit', '')
		params['layer']['extra_fields'] = out.get('units', [])
		params['trend_data'] = report_dict.get('trend_data', {}).items()
		if out.get('y_title'):
			if out.get('unit', ''):
				title = '%s (%s)' %(out.get('y_title', ''), out.get('unit', ''))
			else:
				title = out.get('y_title', '')
		else:
			title = out.get('unit', '')
		params['axis_title']['y_axis_title'] = string.replace(makeChoppedPath(title, 25), BR, '\n')
		if safe_int(report_dict.get('fromMobile')):
			return params
		if out.get('chart_title'):
			params['chart_title']['title'] = out.get('chart_title', '')
		else:
			if report_type in self.pie_charts:
				params['chart_title']['title'] = title
		if out.get('unit') == '%':
			try:
				if maxInputStats.REPORT_Y_AXIS_ADJUST_FOR_PERCENTAGE_UNIT != 1:
					if report_type not in ['area', 'stack_bar_time', 'stack_bar']:
						params['labels']['y_axis_labels'] = (0, 100)
			except:
				if report_type not in ['area', 'stack_bar_time', 'stack_bar']:
					params['labels']['y_axis_labels'] = (0, 100)
		elif out.get('unit') == 'sec' and self.findStatName(report_dict.get('statids',[]),statname='Outage'):
			if report_type not in ['area', 'stack_bar_time', 'stack_bar']:
				params['labels']['y_axis_labels'] = (0, 86400)
		if out.get('max_labels'):
			params['labels']['y_axis_labels'] = (0, safe_int(out.get('max_labels')))
		if params['axis_title'].get('x_axis_title', ''):
			params['axis_title']['x_axis_title'] = ChangeName(params['axis_title']['x_axis_title'])
		params['group_type'] = safe_int(report_dict.get('group_type'))
		# get thresholds
		red_val = safe_float(report_dict.get('report_obj', {}).get('red_val', ''))
		yellow_val = safe_float(report_dict.get('report_obj', {}).get('yellow_val', ''))
		division_factor = report_dict.get('division_factor')
		division = report_dict.get('division', 1000)
		if division != 1000:
			division_factor = self.changeDivisionFactor(division_factor, division)
		if division != 1000 and division_factor:
			division_factor = self.changeDivisionFactor(division_factor, division)
		if division_factor and report_type != 'zarea':
			params['threshold']['red_val'] = red_val / division_factor
			params['threshold']['yellow_val'] = yellow_val / division_factor
		if division_factor and report_type == 'zarea':
			params['threshold']['red_val'] = red_val
			params['threshold']['yellow_val'] = yellow_val
		params['threshold']['polarity'] = report_dict.get('polarity')
		if safe_int(report_dict.get('width', DEFAULT_REPORT_WIDTH)) == LARGE_REPORT_WIDTH:
			params = self.getReportSizeParams(report_type, params, 'large', report_dict=report_dict)
		if safe_int(report_dict.get('width', DEFAULT_REPORT_WIDTH)) == SMALL_REPORT_WIDTH:
			params = self.getReportSizeParams(report_type, params, 'small', report_dict=report_dict)
		if report_dict.get('image_only'):
			params['chart_info']['modify'] = 0
			params['legend_info']['legend_flag'] = 0
			if safe_int(report_dict.get('small_graph')):
				params['small_graph'] = 1
		return params

	def findStatName(self, statids, statname='Outage') :
		"""Matches the given statids with the given statname and return 1 if it is true
		else 0		
		"""
		logMsg(2, 'Enter findStatName with statids-%s,statname-%s ' %(str(statids), statname), self.module_name)
		isPresent = 0
		try :
			if type(statids) == type([]) :
				for statid in statids :
					if self.statid_name_map.get(safe_int(statid)) == statname :
						isPresent = 1
						break
			elif type(statids) == type("") or type(statids) == type(1):
				if self.statid_name_map.get(safe_int(statid)) == statname :
					isPresent = 1
		except Exception, msg :
			logExceptionMsg(4, "Exception in findStatName : %s %s" %(str(Exception), msg), self.single_name)
		logMsg(2, 'Exit findStatName with isPresent-%s' %(isPresent), self.module_name)
		return isPresent

	def changeDivisionFactor(self, division_factor, division):
		""" Change the division factor 1024 to 1000
		"""
		new_div_factor = 1
		while division_factor > 1:
			division_factor = division_factor / division
			new_div_factor = new_div_factor * 1000
		return new_div_factor

	def getReportSizeParams(self, report_type, temp_params, report_size, report_dict={}):
		new_params = copy.deepcopy(temp_params)
		if report_type == 'pie' or report_type == 'donut' or report_type == 'dpie' or report_type == 'rdonut' :
			if hasattr(getReportParams(report_dict), 'report_%s_pie_params' %report_size):
				params_dict = 'getReportParams(report_dict).report_%s_pie_params' %report_size
			else:
				return new_params
		elif report_type == 'pyramid':
			if hasattr(getReportParams(report_dict), 'report_%s_pyramid_params' %report_size):
				params_dict = 'getReportParams(report_dict).report_%s_pyramid_params' %report_size
			else:
				return new_params
		else:
			if hasattr(getReportParams(report_dict), 'report_%s_params' %report_size):
				params_dict = 'getReportParams(report_dict).report_%s_params' %report_size
			else:
				return new_params
		new_params = recursiveupdate(new_params, eval(params_dict))
		return new_params

	def getHalfParams(self, report_type, temp_params,report_dict={}):
		"""Get the params for small graph
		"""
		#half_params = {}
		half_params = copy.deepcopy(temp_params)
		#For Pie & Donut Graphs
		if report_type == 'pie' or report_type == 'donut' or report_type == 'dpie' or report_type == 'rdonut' :
			half_params.update(copy.deepcopy(getReportParams(report_dict).half_pie_params))
		#For other graphs
		else:
			half_params.update(copy.deepcopy(getReportParams(report_dict).half_params))
			half_params['layer']['bar_width'] = (half_params['layer']['bar_width']) / 2
			if half_params.get('threshold') and temp_params.get('threshold'):
				half_params['threshold'].update(temp_params.get('threshold'))
		return half_params

	def makeGraphSummaryTable(self, params, report_dict):
		objs = []
		unit_map = report_dict.get('out', {}).get('legend_unit_map', {})
		summary_objs = params.get('summary_objs', {})
		if summary_objs:
			summary_data_keys = summary_objs.get('keys', [])
			summary_data_objs = summary_objs.get('objs', [])
			headings = []
			for key, heading in summary_data_keys:
				headings.append(('_%s'%key, heading, '20%', 'left'))
			for summary_obj in summary_data_objs:
				obj = {}
				for key, heading in summary_data_keys:
					if key == 'path':
						obj['_%s' %key] = summary_obj.get(key)
					else:
						obj['_%s' %key] = formatValue(summary_obj.get(key), unit_map.get(summary_obj.get('path')))
				objs.append(obj)
		else:
			legend_summary_data = report_dict.get('legend_summary_data', {})
			unit = report_dict.get('out', {}).get('unit', '')
			tz = report_dict.get('report_timezone', '')
			headings = [('_path', 'Resource', '30%', 'left'), ('avg', 'Average', '12%', 'left'), ('min', 'Minimum', '12%', 'left'), ('min_time', 'Min Value Time', '15%', 'left'), ('max', 'Maximum', '12%', 'left'), ('max_time', 'Max Value Time', '15%', 'left')]
			#if isWebPortalEverest():
			#	headings = [('_path', localize('Path','S0069',report_dict.get('language','en')), '30%', 'left'),
			#			('avg', localize('Average','S0070', report_dict.get('language','en')), '12%', 'left'),
			#			('min', localize('Minimum','S0071', report_dict.get('language','en')), '12%', 'left'),
			#			('min_time', localize('Min Value Time','S0072', report_dict.get('language','en')), '15%', 'left'),
			#			('max', localize('Maximum','S0073', report_dict.get('language','en')), '12%', 'left'),
			#			('max_time', localize('Max Value Time','S0074', report_dict.get('language','en')), '15%', 'left')
			#		]
			###Specifically for IMSS -- Not the best way to sort the data.
			cos_post_keys = [ 'Average Drop Traffic', 'Maximum Drop Traffic', 'Minimum Drop Traffic','Average Cos Traffic Post Policy', 'Maximum Cos Traffic Post Policy', 'Minimum Cos Traffic Post Policy']
			cos_pre_keys = [ 'Average Drop Traffic', 'Maximum Drop Traffic', 'Minimum Drop Traffic','Average Cos Traffic Pre Policy', 'Maximum Cos Traffic Pre Policy', 'Minimum Cos Traffic Pre Policy']
			summary_data_keys = legend_summary_data.keys()
			summary_data_keys.sort()
			cos_post_keys_sorted = [ 'Average Drop Traffic', 'Maximum Drop Traffic', 'Minimum Drop Traffic','Average Cos Traffic Post Policy', 'Maximum Cos Traffic Post Policy', 'Minimum Cos Traffic Post Policy']
			cos_post_keys_sorted.sort()
			cos_pre_keys_sorted = [ 'Average Drop Traffic', 'Maximum Drop Traffic', 'Minimum Drop Traffic','Average Cos Traffic Pre Policy', 'Maximum Cos Traffic Pre Policy', 'Minimum Cos Traffic Pre Policy']
			cos_pre_keys_sorted.sort()
			if  cos_pre_keys_sorted == summary_data_keys :	#Pre-policy Traffic [COS]
				summary_data_keys = cos_pre_keys
			if cos_post_keys_sorted == summary_data_keys:	#Post-policy Traffic[COS]
				summary_data_keys = cos_post_keys
			if summary_data_keys != ['Cos Traffic Post Policy','Drop Traffic'] and summary_data_keys != ['Cos Traffic Pre Policy','Drop Traffic']:	#Don reverse sort the COS Realtime Traffic Reports
				summary_data_keys.reverse()	#The Data displayed like minimum/maximum/average by default.
			for key in summary_data_keys:
				#Added for sorting based on the keys defined.
				summary_obj = legend_summary_data.get(key,{})			
				obj = {}
				obj['_path'] = summary_obj.get('_path', '')
				obj['avg'] = formatValue(summary_obj.get('avg', ''), unit)
				obj['min'] = formatValue(summary_obj.get('min', ''), unit)
				obj['max'] = formatValue(summary_obj.get('max', ''), unit)
				obj['min_time'] = makeYYYYmmddTime(summary_obj.get('min_time', ''))
				obj['max_time'] = makeYYYYmmddTime(summary_obj.get('max_time', ''))			
				objs.append(obj)
		return XMLReportDataAndSummaryTable(headings, objs, width=params.get('width', SMALL_REPORT_WIDTH))

	def makeDataTable(self, params, request, isFormatChange=0):
		"""Draw the data table
		"""
		time_stamps = params.get('data_time_stamps')
		datas = params.get('data')
		unit_map = params.get('legend_unit_map')
		time_format = params.get('data_time_format')
		excel_time_format = {
						'%H:%M:%S': '_time',
						'%H:%M': '_time',
						'%H:%M:00': '_time',
						'%H:00': '_time',
						'%m/%d/%Y %H:%M:%S': '_timeStamp', 
						'%m/%d/%Y %H:%M:00': '_timeStamp', 
						'%m/%d/%Y %H:%M': '_timeStamp', 
						'%m/%d/%Y %H:00': '_timeStamp',
						'%m/%d/%Y': '_date',
					}
		time_key = excel_time_format.get(time_format, '_dtime')
		red_val = safe_float(request.get('report_obj', {}).get('red_val', ''))
		yellow_val = safe_float(request.get('report_obj', {}).get('yellow_val', ''))
		polarity = -1	
		if red_val and yellow_val:
			polarity = iif(red_val >= yellow_val, 1, -1)
		threshold = [yellow_val, red_val, polarity]
		if request.get('fromPDF'):
			chart_time_format = params.get('time_format', '')
		else:
			chart_time_format = string.replace(params.get('time_format', ''), ' ', '&nbsp;')
		#chart_time_format = string.replace(params.get('time_format', ''), ' ', '&nbsp;')
		#chart_time_format = string.replace(chart_time_format, '00', 'mm')
		#chart_time_format = string.replace(chart_time_format, 'nn', 'mm')
		excel_chart_time_format = string.strip(string.replace(params.get('time_format', ''), 'nn', 'mm'))
		chart_time_format = string.strip(string.replace(chart_time_format, 'nn', 'mm'))
		if time_format.endswith(':%M'):
			chart_time_format += ':ss'
			excel_chart_time_format += ':ss'
			time_format += ':%S'
		paths = unit_map.keys()
		col_width = 96 / (len(paths))
		time_col_width = '4%'
		if not col_width:
			time_col_width = str(col_width) + '%'
		try:
			headings = [('time', interText.interString['S5225'] %(chart_time_format), time_col_width, 'center')]
		except:
			headings = [('time', 'Time %s' %(chart_time_format), time_col_width, 'left')]
		csv_headings = [(time_key, 'Time %s' %(excel_chart_time_format), 25)]
		# Getting headings dynamically, based on the paths available
		index = 0
		for path in paths:
			#<Robinson Panicker : 20130130: Adding the Resource or Statistic name to the heading>
			if params.get('chart_title'):
				path = path + '(%s)' %params.get('chart_title', '')
			#<Robinson Panicker : 20130130: Adding the Resource or Statistic name to the heading ends here>
			headings.append(('path_%s' %(index), makeChoppedPath(path, 15), str(col_width) + '%', 'center'))
			csv_headings.append(('_path_%s' %(index), makeChoppedPath(path, 20), 25))
			index += 1
		# Prepare time based datas
		time_format = string.replace(time_format, ' ', '&nbsp;')
		# For CSV Report
		if safe_int(request.get('format_csv')) == 2:
			csv_headings = headings[:]
		time_dict = {}
		legend_summary_map = {}
		timeStampToFormattedTime = {}
		formattedTimeToTimeStamp = {}
		for legend, data in datas:
			data_value = map(lambda a: a[1], data)
			# Customized Data Table for Displaying Average/Min/Max of Columns
			if maxInputStats.REPORT_COLUMN_SUMMARY_DATA:
				if not legend_summary_map.has_key(legend):
					legend_summary_map[legend] = {'avg': '-', 'min': '-', 'max': '-'}
				legend_summary_map[legend]['avg'] = '%.2f' %(avg(map(lambda a: safe_float(a), filter(lambda a: a != NULL_VALUE, data_value))))
				legend_summary_map[legend]['max'] = '%.2f' %(max(map(lambda a: safe_float(a), filter(lambda a: a != NULL_VALUE, data_value))))
				legend_summary_map[legend]['min'] = '%.2f' %(min(map(lambda a: safe_float(a), filter(lambda a: a != NULL_VALUE, data_value))))
			for time_stamp, value in data:
				if value == NULL_VALUE:
					continue
				formatted_time = time.strftime(time_format, time.localtime(time_stamp))
				if not time_dict.has_key(formatted_time):
					time_dict[formatted_time] = {}
				if not timeStampToFormattedTime.has_key(time_stamp) :
					timeStampToFormattedTime[time_stamp] = formatted_time
				time_dict[formatted_time][legend] = value
		for time_stamp, formatted_time in timeStampToFormattedTime.items():
			formattedTimeToTimeStamp[formatted_time] = time_stamp
		objs = []
		# sorting the datas based on the time.
		# TODO: need to do better solution
		report_time_stamps = timeStampToFormattedTime.items()
		report_time_stamps.sort(sortTuple1)
		temp_times = map(lambda a: a[1], report_time_stamps)
		formatted_report_times = []
		formatted_report_time_map = {}
		for temp_time in temp_times:
			if not formatted_report_time_map.has_key(temp_time):
				formatted_report_times.append(temp_time)
				formatted_report_time_map[temp_time] = 1
		temp_datas = []
		for formatted_time in formatted_report_times:
			temp_datas.append((formatted_time, time_dict[formatted_time]))
		# Construct the objs
		path_index_map = {}
		index = 0
		for path in paths:
			path_index_map[path] = index
			index += 1
		gif_prefix = request.get("severity_gif_name", request['request']["__user"].get("default_gif", "smiley"))
		division_factor = request.get('division_factor')
		actual_legend_unit_map = {}
		map(lambda a, b=actual_legend_unit_map: b.update(a), request.get('legend_unit_map', {}).values())
		for time_stamp, datas in temp_datas:
			obj = {'time': time_stamp}
			# Added For Excel Time Formatting.
			lt = time.localtime(formattedTimeToTimeStamp.get(time_stamp))
			obj[time_key] = DateTime.DateTime(lt[0], lt[1], lt[2], lt[3], lt[4], lt[5])
			# For Excel Time Formatting ends here
			for path, data in datas.items():
				temp_data = data
				if isFormatChange:
					data = formatValue(safe_int(data),isFormatChange)
				elif data: 
					data = formatValue(data, unit_map.get(path))
				if not request.get('fromPDF') and request.get('topn'):
					actual_unit = actual_legend_unit_map.get(path)
					if actual_unit in self.units_to_be_formatted and actual_unit != 'sec':
						temp_division_factor = division_factor
					else:
						temp_division_factor = 1
					color_index = self.getThresholdColor(threshold, safe_float(temp_data) * temp_division_factor)
					data = string.replace(data, ' ', '&nbsp;')
					if color_index != -1:
						data = [XMLImage(self.severityMap[color_index], border=0), XMLSpace(2), data]
				else:
					data = formatValue(temp_data, unit_map.get(path))
				csv_data = formatValue(temp_data, unit_map.get(path))
				obj['path_%s' %(path_index_map.get(path))] = data
				obj['_path_%s' %(path_index_map.get(path))] = csv_data
				index += 1
			objs.append(obj)
		# Customized Data Table for Displaying Average/Min/Max of Columns
		if maxInputStats.REPORT_COLUMN_SUMMARY_DATA:
			objs.append({})
			objs.append({})
			try:
				summary_val = [('avg', interText.interLink['L203']), ('max', interText,interString['S5204']), ('min', interText.interLink['L205'])]
			except:
				summary_val = [('avg', 'Average'), ('max', 'Maximum'), ('min', 'Minimum')]
			for key, summary in summary_val:
				obj = {'time': XMLSpan(summary, classId='normal_text_title')}
				for path in paths:
					obj['path_%s' %(path_index_map[path])] = '%s&nbsp;%s' %(legend_summary_map[path][key], unit_map.get(path))
				objs.append(obj)
			objs.append({})
			objs.append({})
		# Customization Ends here 
		width = params.get('width', DEFAULT_REPORT_WIDTH)
		#Get the height based on the width
		if width > SMALL_REPORT_WIDTH:
			if len(objs) < 15:
				height = (len(objs) * 16) + 50
			else:
				height = 400
		else:
			height = 400
			# Add dummy entries, so that the table looks better when compare to the others.
			if len(objs) < 15 and safe_int(request.get("fromPDF",0)):
				self.addDummyObjs(headings, objs)
		if len(headings) > 21 :
			request['no_resource'] = len(headings)
		elif len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY:
			request['no_resource'] = len(objs)			
		return XMLReportDataAndSummaryTable(headings, objs, data=2, width=width, csv_schema=csv_headings)

	def addDummyObjs(self, headings, objs):
		"""Add dummy objs for the data table.
		"""
		for i in range((15 - len(objs))):
			obj = {}
			for key, name, width, align in headings:
				obj[key] = ''
			objs.append(obj)

	def makeSummaryTable(self, params, request):
		"""Draw the summary table.
		"""
		ip_datas = params.get('data', [])
		unit_map = params.get('legend_unit_map')
		report_link_map = params.get('legend_report_link_map', {})
		red_val = safe_float(request.get('report_obj', {}).get('red_val', ''))
		yellow_val = safe_float(request.get('report_obj', {}).get('yellow_val', ''))
		polarity = -1
		if red_val and yellow_val:
			polarity = iif(red_val >= yellow_val, 1, -1)
		threshold = [yellow_val, red_val, polarity]
		heading = 'Path'
		if safe_int(request.get('group_type')) == 1:
			heading = 'Statistics'
		try:
			headings = [('path', ChangeName(heading), '40%', 'left'), ('avg', interText.interLink['L203'], '15%', 'left'), ('min', interText.interLink['L205'], '15%', 'left'), ('max', interText.interString['S5214'], '15%', 'left'), ('percentile_95', '95th Percentile', '15%', 'left')]
		except:
			headings = [('path', ChangeName(heading), '40%', 'left'), ('avg', 'Average', '15%', 'left'), ('min', 'Minimum', '15%', 'left'), ('max', 'Maximum', '15%', 'left'), ('percentile_95', '95th Percentile', '15%', 'left')]
		csv_headings = [
				('path', heading, 65),
				('avg', 'Average', 15),
				('min', 'Minimum', 15),
				('max', 'Maximum', 15),
				('percentile_95', '95th Percentile', 15),
			]
		objs = []
		count = 0
		char = iif(params.get('width', SMALL_REPORT_WIDTH) == SMALL_REPORT_WIDTH, 25, 50)
		division_factor = request.get('division_factor')
		actual_legend_unit_map = {}
		map(lambda a, b=actual_legend_unit_map: b.update(a), request.get('legend_unit_map', {}).values())
		ip_datas1=[]
		ip_datas1=sorted(ip_datas,key=lambda x:float(x[1][3]),reverse=True)
		for legend, datas in ip_datas1:
			obj = {}
			min_val, max_val, avg_val, percentile_95 = datas
			path = legend
			additional_headings = []
			if params.get('chart_title'):
				path = legend + '(%s)' %params.get('chart_title', '')
				additional_headings = ['%s Report' %params.get('chart_title', '')]
			if request['request'].get('fromPDF'):
				obj['path'] = path
				obj['min'] = formatValue(min_val, unit_map.get(legend))
				obj['max'] = formatValue(max_val, unit_map.get(legend))
				obj['avg'] = formatValue(avg_val, unit_map.get(legend))
				obj['percentile_95'] = formatValue(percentile_95, unit_map.get(legend))
			else:
				obj['path'] = XMLSpan(path[:char], title=path)
				obj['min'] = XMLSpan(formatValue(min_val, unit_map.get(legend)))
				obj['max'] = XMLSpan(formatValue(max_val, unit_map.get(legend)))
				obj['avg'] = XMLSpan(formatValue(avg_val, unit_map.get(legend)))
				obj['percentile_95'] = XMLSpan(formatValue(percentile_95, unit_map.get(legend)))
			# if show all option is selected or for PDF Reports, dont show the gifs.
			if not request['request'].get('fromPDF'):
				actual_unit = actual_legend_unit_map.get(legend)
				if actual_unit in self.units_to_be_formatted and actual_unit != 'sec':
					temp_division_factor = division_factor
				else:
					temp_division_factor = 1
				report_link = report_link_map.get(legend)
				if report_link:
					report_link += '&%s' %(self.getSelectedTime(request.get('request')))
					obj['path'] = XMLLink('javascript:openAdHocReport(&quot;selectgroupreport?%s&quot;)' %(report_link), legend[:char], Title=legend, classId='commons1')
				if safe_int(request.get('topn')):
					color_index = self.getThresholdColor(threshold, safe_float(min_val) * temp_division_factor)
					if color_index != -1:
						obj['min'] = [XMLImage(self.severityMap[color_index], border=0), XMLSpace(1), XMLSpan(formatValue(min_val, unit_map.get(legend)))]
					color_index = self.getThresholdColor(threshold, safe_float(max_val) * temp_division_factor)
					if color_index != -1:
						obj['max'] = [XMLImage(self.severityMap[color_index], border=0), XMLSpace(1), XMLSpan(formatValue(max_val, unit_map.get(legend)))]
					color_index = self.getThresholdColor(threshold, safe_float(avg_val) * temp_division_factor)
					if color_index != -1:
						obj['avg'] = [XMLImage(self.severityMap[color_index], border=0), XMLSpace(1), XMLSpan(formatValue(avg_val, unit_map.get(legend)))]
			objs.append(obj)
			count += 1
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY:
			request['no_resource'] = len(objs)		
		return XMLReportDataAndSummaryTable(headings, objs, width=params.get('width', SMALL_REPORT_WIDTH), csv_schema=csv_headings, additional_headings=additional_headings)

	def getThresholdColor(self, threshold, value) :
		try :
			if threshold[0] == threshold[1] and threshold[0] == 0.0:
				return -1
			state = 0
			if threshold[2] == -1 :
				#Negative Polarity
				if value < threshold[1] :
					state = 2
				elif value < threshold[0] :
					state = 1
			else :
				#Positive Polarity
				if value > threshold[1] :
					state = 2
				elif value > threshold[0] :
					state = 1
			return state
		except Exception, msg :
			logExceptionMsg(4, "Exception in getThresholdColor : %s %s" %(str(Exception), msg), self.single_name)
			return -1

	def checkStatCount(self):
		sql_stmt = "select count(statid) count from tblStatMap"
		objs = DatabaseServer.executeSQLStatement(sql_stmt)
		if objs and type(objs) == type([]):
			if safe_int(objs[0]["count"]) != self.stat_count:
				self.updateStatMapObjs()
	
	def modifyGroupReport(self, request):
		"""Modify the particular group report.
		"""
		try:
			if request.get('report_type', '') == 'node_summary' or request.get('report_type', '') == 'node_res_summary': 
				request.update({'statids_in_order': request.get('statids')})
			if request.get('report_type', '') == 'pattern': 
				if safe_int(request.get('resolution')):
					request['pattern_type'] = safe_int(request.get('resolution'))
			if maxInputStats.ENABLE_C3JS_GRAPH:
				request['overview'] = 1
				request['c3js'] = 1
			report_dict = {}
			report_dict.update(request)
			report_dict['request'] = request
			template = 'blanktemplate.htm'
			report_dict['ret'] = {'template': template}
			self.checkStatCount()
			self.getAdHocReportObj(report_dict)
			return self.basePublishObjs(report_dict)
		except Exception, msg:
			msg = 'Exception while generating the report - %s' %(msg)
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report')}
		except NoReportException:
			msg = ChangeName('No Report is configured. Please configure some report')
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report')}
		except:
			msg = 'Exception while generating the report'
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report')}

	def selectGroupReport(self, request):
		"""On select of any group report.
		"""	
		try:
			user_info = request.get('__user', {})
			if isPortalAccount(user_info):
				raise XMLNoPriv, interText.interString["S031"]
			# For Pattern Report
			st_time = time.time()
			self.checkStatCount()
			if request.get('report_type', '') == 'pattern': 
				if safe_int(request.get('resolution')):
					request['pattern_type'] = safe_int(request.get('resolution'))
			if request.get('report_type', '') == 'node_summary' or request.get('report_type', '') == 'node_res_summary': 
				request.update({'statids_in_order':request.get('statids')})
			# For Calendar
			request['calendar_pos'] = (708, 84)
			request['popup_report'] = 1
			request['header_class'] = 'Reports_popup'
			request['init_option'] = 1
			if request.has_key("c3js") and safe_int(request.get("c3js"))==1:
				if request.get('report_type') in ['line','area','spline','bar','line_bar']:
					request['c3_report_type'] = request.get('report_type')
					request['report_type'] = 'trend'
					if request.get("c3_report_type") in ['line_bar','current_history']:
						request['display'] = 'all'
			report_dict = {}			
			report_dict.update(request)
			report_dict['timebetween'] = ''
			report_dict['request'] = request
			template = 'report_popup.htm'
			if request.has_key('template'):
				template = request.get('template', '')
				if not template:
					template = 'report_popup.htm'
			report_dict['ret'] = {'template': template}				
			self.getAdHocReportObj(report_dict)
			if (report_dict.get('report_list_obj').get('report_type') == 'node_summary' or report_dict.get('report_list_obj').get('report_type') == 'node_res_summary'):
				request.update({'statids_in_order':report_dict.get('statids')})	
				report_dict.update({'statids_in_order':str(report_dict.get('statids'))})
			if report_dict.get('report_list_obj').get('report_type') == 'statewise_summary':
				self.vsatStateWiseFilter(report_dict)
			self.getTimeFilters(report_dict) #added to update the time filters
			self.basePublishObjs(report_dict)
			self.getFormElements(report_dict)			
			ret = report_dict.get('ret', {})
			request['time_durations'] = self.time_scale
			ret['time_scale'] = [XMLJSLib('js/timescale.js'), self.left_panel.getTimeControls(request), XMLJSLib('js/report.js')]
			ret['hiddenFields'] = report_dict.get('hidden_fields', []) + [
									XMLHiddenField('group_id', request.get('group_id', '')),
									XMLHiddenField('resfilter', string.replace(report_dict.get('resfilter', ''), '"', '&quot;'), 'resfilter'),
									XMLHiddenField('View_type', safe_int(report_dict.get('report_obj', {}).get('viewtype'))),
								]
			ret['navigation_url'] = 'navigate?View_type=%d&is_not_default=1' %(safe_int(report_dict.get('report_obj', {}).get('viewtype')))
			ret['statlist_url'] = 'statlist?report_type=%s' %(report_dict.get('report_type', ''))
			min_max_array = {'Minimize': ChangeName('Minimize'), 'Maximize': ChangeName('Maximize'), 'Display_One': ChangeName('Display One'), 'Display_All': ChangeName('Display All')}
			resolution_array = {
								'Select': ChangeName('Select Resolution'), 'Minute': ChangeName('Minute'), 'Hour': ChangeName('Hour'), 'Day': ChangeName('Day'), 'Month': ChangeName('Month'),
								'Select_Pattern': ChangeName('Select Pattern'), 'Hour_Day': ChangeName('Hour of Day'), 'Day_Week': ChangeName('Day of Week'), 'Day_Month': ChangeName('Day of Month'), 'Month_Year': ChangeName('Month of Year'),
						}
			js_content = XMLJS('var MIN_MAX = %s;\n var RESOLUTION = %s;\n' %(convertPyDictToJSMap(min_max_array), convertPyDictToJSMap(resolution_array)))
			report_content = ret['content']
			ret['content'] = [js_content]
			ret['content'].append(XMLCSSLib('C3/css/c3.min.css'))
			ret['content'].append(XMLJSLib('C3/js/d3.min.js'))
			ret['content'].append(XMLJSLib('C3/js/c3.min.js'))
			ret['content'].append(XMLJSLib('C3/C3JSChart.js'))	
			ret['content'].append(report_content)	
			if report_dict.has_key('display_str') and not report_dict.get('autoReport'):
				ret['content'].append(XMLJS("alert('%s')" %(report_dict['display_str'])))
			ret['title'] = ChangeName(self.plural_name)
			if ((request.get('report_type','') == 'data') and (safe_int(request.get('topn',1)) ==0 or safe_int(report_dict.get('report_obj', {}).get('topn',1)) ==0  or safe_int(report_dict.get('no_resource',1)) > 21 )) and (not report_dict.get('fromPDF')):
				content = self.generatePDF(request,ret)
				content_replace = XMLContentTable([XMLTable(content, classId='alignedTableForm')], table_heading=ret.get('title', ''))
				ret['content'] = self.getContentReplace(ret['content'],content_replace)
			long_reports = ['downtime_report','data','summary','events','device_downtime_summary_report','traps_summary','traps','nodes_summary','vsatpingreport','nw_efficiency','nw_statistic','uptime_summary']
			if (report_dict.get('report_type','') in long_reports and safe_int(report_dict.get('no_resource',1)) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY) and (not report_dict.get('fromPDF')):
				content = self.generatePDF(request,ret)
				content_replace = XMLContentTable([XMLTable(content, classId='alignedTableForm')], table_heading=ret.get('title', ''))
				ret['content'] = self.getContentReplace(ret['content'],content_replace)
			et_time = time.time()
			logMsg(2, 'Time taken to generate the Report - %s' %(`et_time - st_time`), self.module_name)
			return ret
		except CallNetflowReportException, msg:
			# TO call the Netflow Report
			return {"content" : XMLRedirect('nfReport?reportdn=%s'%msg)}
		except NoReportException:
			msg = ChangeName('No Report is configured. Please configure some report')
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report')}
		except Exception, msg:
			msg = 'Exception while generating the report - %s' %(msg)
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report')}
		except:
			msg = 'Exception while generating the report'
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report')}

	def getAdHocReportObj(self, report_dict):
		""" Get Ad-hoc report objs
		"""
		report_obj = {}
		report_list_obj = {}
		view_type = safe_int(report_dict.get('View_type'))
		hiddenFields = []
		# For Resource View		
		if view_type == 0:
			root_path_key = 'root_path'
		# For Location View
		elif view_type == 1:
			root_path_key = 'loc_path'
		# For Domain View
		elif view_type == 2:
			root_path_key = 'dom_path'
		elif view_type == 3:
			root_path_key = 'biz_path'
		user_root_path = report_dict.get('__user', {}).get(root_path_key, ROOT_PATH)
		if report_dict.has_key('group_id') and safe_int(report_dict.get('group_id')) > 0:
			# come from report page, on select of detach report
			report_list_obj = self.report_profile.getReportListObj(safe_int(report_dict.get('group_id')))
			report_id = safe_int(report_list_obj.get('reportid'))
			report_obj = self.report_profile.getReportObj(report_id)
		else:
			# come from all other pages
			res_filter = []
			filters = ['resid', 'res_name', 'poll_addr', 'alias', 'profile', 'restype', 'node_type', 'device_name', 'node_alias']
			for key in filters:
				if report_dict.has_key(key) and report_dict.get(key) not in [None,"None",'None']:
					res_filter.append(' %s = "%s" ' %(key, report_dict.get(key)))
					hiddenFields.append(XMLHiddenField(key, report_dict.get(key)))
			res_filters = string.join(res_filter, ' AND ')
			report_obj = {
					'reportid': -1, 'name': '', 'category': 'AdHoc Reports', 'pathFmt': 0, 'viewtype': view_type,
					'pathnames': '', 'user_list': 'Private', 'timesel': report_dict.get('timesel',2), 'resfilter': res_filters, 'topn': 4,'timebetween':'',
				}
		# update report obj
		report_obj_datas = ['name', 'category', 'pathnames', 'resfilter', 'topn', 'business_hr', 'business_hr_profile', 'resolution','timebetween']
		#added for the Vmax USer report fix -- bug 556 -- Everest 3.0
		for data in report_obj_datas:
			if report_dict.has_key(data) and report_dict.get(data, ''):
				report_obj[data] = report_dict.get(data)
			else:
				report_dict[data] = report_obj.get(data)
			if data == 'pathnames':
				result = ''
				val = report_dict.get(data, '')
				if user_root_path.startswith('root/'):
					user_root_path = user_root_path[5:]
				if not val is None:
					val = val.strip()
				if val != '' and not val is None:
					if val.find(' | '):
						values = val.split(' | ')
						for value in values:
							if value.startswith('root/'):
								value= value[5:]							
							if value.startswith(user_root_path) or value.startswith(user_root_path + PATH_SEP):
								result += value + ' | '
							else:
								result += user_root_path + PATH_SEP + value + ' | '
						if result.endswith(' | '):
							result = result[:-2]
				report_obj[data] = result
			else:
				report_dict[data] = report_obj.get(data)
				if data == 'pathnames':
					result = ''
					val = report_obj.get(data, '')
					if user_root_path.startswith('root/'):
						user_root_path = user_root_path[5:]
					val = val.strip()
					if val != '':
						if val.find(' | '):
							values = val.split(' | ')
							for value in values:
								value=value.strip()
								if value:
									if value.startswith('root/'):
										value= value[5:]									
									if value.startswith(user_root_path) or value.startswith(user_root_path + PATH_SEP)  :
										result += value + ' | '
									else:
										result += user_root_path + PATH_SEP + value + ' | '
							if result.endswith(' | '):
								result = result[:-2]
					report_obj[data] = result
		#added for the Vmax USer report fix -- bug 556 -- Everest 3.0
		# update report list obj
		red_val = safe_float(report_dict.get('red_val', report_list_obj.get('red_val', '')))
		yellow_val = safe_float(report_dict.get('yellow_val', report_list_obj.get('yellow_val', '')))
		width = report_dict.get('width', report_list_obj.get('width', report_obj.get('width',"")))
		display = report_dict.get('display', report_list_obj.get('display', 'avg'))
		occurance = safe_int(report_dict.get('occurance', report_list_obj.get('occurance', report_obj.get('occurance',0))))
		stat_unit = safe_int(report_dict.get('stat_unit', report_list_obj.get('stat_unit', report_obj.get('stat_unit', -1))))
		if not width:
			width = DEFAULT_REPORT_WIDTH
		report_dict['width'] = width
		report_type = report_dict.get('report_type', report_list_obj.get('reporttype', ''))
		report_list_id = safe_int(report_dict.get('report_list_id',report_list_obj.get('reportlistid', '')))
		timebetween = report_dict.get('timebetween', report_list_obj.get('timebetween', ''))
		temp_report_list_obj = {
								'name': report_dict.get('name', report_list_obj.get('name', '')),
								'group_type': report_dict.get('group_type', report_list_obj.get('group_type', '')),
								'reporttype': report_type,
								'report_type': report_type,
								'red_val': red_val,
								'yellow_val': yellow_val,
								'width': width,
								'height': getReportParams(report_dict).report_height,
								'font': report_dict.get('font', report_list_obj.get('font', '')),
								'pval': report_dict.get('pval', iif(red_val >= yellow_val, 1, -1)),
								'display' : display,
								'timebetween':timebetween,
								'reportlistid': report_list_id,
								'occurance': occurance,
								'stat_unit': stat_unit, 
							}
		# check for statids in the request
		if report_dict.has_key('statids') and report_dict.get('statids'):
			statistics = report_dict.get('statids')
			if statistics.startswith('['):
				statids = eval(statistics)
			else:
				statids = string.split(statistics, ',')
			for statid in statids:
				# if suppose, statid is not in memory, then add it dynamically.
				if not self.statid_name_map.has_key(safe_int(statid)):
					self.updateStatInMemory(statid)
			stats = string.join(map(lambda a, b=self.statid_name_map: b.get(safe_int(a)), statids), ' | ')
		else:
			# if not,check fot stats in the request
			stats = report_dict.get('stats', '')
			if not stats:
				# if not, get it from report list obj
				stats = report_list_obj.get('stats', '')
			statids = map(lambda a, b = self.statname_id_map: safe_int(b.get(a.strip())), string.split(stats, ' | '))
		# assign both stats & statids into request
		statids = map(lambda a: safe_int(a), statids)
		temp_report_list_obj['statids'] = statids
		report_dict['stats'] = stats
		report_dict['statids'] = statids
		# update rport dict with the datas
		report_dict['resolution'] = report_obj.get('resolution')
		report_dict['topn'] = safe_int(report_obj.get('topn', 4))
		report_dict['timebetween'] = report_obj.get('timebetween', '')
		report_dict['business_hr'] = report_obj.get('business_hr', 0)
		report_dict['business_hr_profile'] = report_obj.get('business_hr_profile', 0)
		report_dict['report_type'] = temp_report_list_obj.get('report_type', 'trend')
		report_dict['display'] = temp_report_list_obj.get('display', 'avg')
		report_dict['sub_reports'] = [temp_report_list_obj]
		report_dict['report_list_obj'] = temp_report_list_obj
		report_dict['report_obj'] = report_obj
		report_dict['hidden_fields'] = hiddenFields

	def getFormElements(self, request):
		"""construct the form elements from the request.
		"""
		ret = {}
		# paths
		#added for the Vmax USer report fix -- bug 556 -- Everest 3.0
		view_type = safe_int(request.get('View_type'))
		hiddenFields = []
		# For Resource View
		if view_type == 0:
			root_path_key = 'root_path'
		# For Location View
		elif view_type == 1:
			root_path_key = 'loc_path'
		# For Domain View
		elif view_type == 2:
			root_path_key = 'dom_path'
		elif view_type == 3:
			root_path_key = 'biz_path'
		user_root_path = request.get('__user', {}).get(root_path_key, ROOT_PATH)	
		# paths
		#path_names = makeRelativePath(request.get('pathnames', ''), request.get('root_path', 'root'))
		#user_root_path = iif(user_root_path.startswith('root/'),user_root_path[5:],user_root_path)
		pathnames = request.get('pathnames', '') 
		if pathnames == None:
			pathnames = ''			
		if pathnames != '':
			user_root_path = iif(user_root_path.startswith('root/'),user_root_path[5:],user_root_path)
		#path_names = makeRelativePath(pathnames ,  user_root_path)
		if pathnames.find(' | '):
			result =''
			values = pathnames.split(' | ')
			for value in values:
				value = value.strip()
				if value:
					if value.startswith('root/'):
						value = value[5:]					
					if value.startswith(user_root_path+'/'):
						value = value[len(user_root_path) + 1:]
					if value.startswith(user_root_path):
						value = value[len(user_root_path):]						
					if value.startswith(user_root_path) or value.startswith(user_root_path + PATH_SEP)  :
						result += makeRelativePath(value,  user_root_path)+ ' | '
					else:
						result += makeRelativePath(value,  user_root_path) + ' | '
			if result.endswith(' | '):
				result = result[:-2]	
			pathnames = result
		path_names = makeRelativePath(pathnames, user_root_path)
		if path_names == None:
			path_names = ''	
		#added for the Vmax USer report fix -- bug 556 -- Everest 3.0
		ret['pathnames'] = XMLTextArea('pathnames', path_names, id='pathnames_div')
		ret['stats'] = XMLTextArea('stats', request.get('stats', ''), id='stats_div')
		# texts used in template.
		ret['resfolder'] = ChangeName('Resources/Folders')
		ret['statistics'] = ChangeName('Statistics')
		ret['clear'] = ChangeName('Clear')
		ret['submit'] = ChangeName('Generate Report')
		ret['save_as_new'] = ChangeName('Save As New Report')
		ret['config'] = ChangeName('Configuration')
		ret['minimize'] = ChangeName('Minimize')
		obj = {}
		report_list_obj_datas = ['report_type', 'group_type', 'width', 'red_val', 'yellow_val', 'display', 'occurance', 'stat_unit']
		report_obj_datas = ['name', 'category', 'resfilter', 'topn', 'business_hr', 'business_hr_profile', 'timebetween']
		# report list obj information
		for data in report_list_obj_datas:
			if request.get(data):
				obj[data] = request.get(data)
			else:
				obj[data] = request['report_list_obj'].get(data)
		# report obj information
		for data in report_obj_datas:
			if request.get(data):
				obj[data] = request.get(data)
			else:
				obj[data] = request['report_obj'].get(data)
		obj['resolution'] = request.get('resolution', request['report_obj'].get('resolution'))
		# group type
		group_type_list = getReportParams(request).group_type_map.items()
		group_type_list.sort(sortTuple1)
		# top n
		top_options = getReportParams(request).top_option
		top_options.sort(sortTuple1)
		# report type
		report_types = getReportParams(request).report_map.items()
		report_types.sort(sortTuple2)
		report_types.insert(0, ('', 'Select Report Type'))
		# chart size
		chart_size = [(LARGE_REPORT_WIDTH, 'Large'), (DEFAULT_REPORT_WIDTH, 'Medium'), (SMALL_REPORT_WIDTH, 'Small')]
		# diplay 
		display = [('avg', 'Average'), ('min', 'Minimum'),('max', 'Maximum'), ('all', 'All')]
		display_type_option = ''
		unit_options = [(-1,'Select'),(0,'Bytes / bps'),(1,'KBytes / Kbps'),(2,'MBytes / Mbps'),(3,'GBytes / Gbps')]
		if maxInputStats.AVG_MIN_MAX_IN_REPORT:
			display_type_option = (XMLSpan('Display', classId='normal_text_title'), [XMLDropDown('display', display, obj.get('display'), classId='dropdown_common')])
		report_group_types = getReportParams(request).report_group_map[:]
		report_group_types.insert(0, ('', [('', 'Selet Report Type')]))
		report_type_dropdown = XMLGroupDropDown('report_type', report_group_types, obj.get('report_type', ''), classId='dropdown_common dropdown_medium')
		# report_type_dropdown = XMLDropDown('report_type', report_types, obj.get('report_type', ''), classId='dropdown_common dropdown_medium')
		name_filter = [
					[[XMLSpace(1), XMLSpan('Report Name', classId='normal_text_title')], XMLTextField('name', obj.get('name', ''), classId='textfield_popup')],
					[[XMLSpace(1), XMLSpan('Report Category', classId='normal_text_title')], XMLTextField('category', obj.get('category', ''), classId='textfield_popup')],
					['', [XMLLink("javascript:openResourceFilter('getresourcefilters?View_type=%s');"%(safe_int(request.get('View_type'))), ChangeName('Change Filters'), js=1, classId='normal_text_title'), XMLSpace(30), XMLLink('javascript:clear(&quot;resfilter&quot;);', ChangeName('Clear'), js=1, classId='normal_text_title')], ''],
					[[XMLSpace(1), XMLSpan('Filters', classId='normal_text_title')], '<DIV id="resfilter_div">%s</DIV>' %(obj.get('resfilter'))],
			]
		report_options = [
					[[XMLSpace(1), XMLSpan('Type', classId='normal_text_title')], [XMLDropDown('group_type', group_type_list, safe_int(obj.get('group_type')), classId='dropdown_common dropdown_medium')], [XMLSpace(1), XMLSpan('Graph', classId='normal_text_title')], [report_type_dropdown]],
					[[XMLSpace(1), XMLSpan('Top N', classId='normal_text_title')], [XMLDropDown('topn',top_options, safe_int(obj.get('topn')), classId='dropdown_common dropdown_medium')], [XMLSpace(1), XMLSpan('Resolution', classId='normal_text_title')], [XMLDropDown('resolution',getReportParams(request).resolution, obj.get('resolution'), classId='dropdown_common dropdown_medium')]],
					[[XMLSpace(1), XMLSpan('Scale', classId='normal_text_title')], [XMLDropDown('business_hr', getReportParams(request).business_hr_options, obj.get('business_hr'), classId='dropdown_common dropdown_medium')], [XMLSpace(1), XMLSpan('Size', classId='normal_text_title')], [XMLDropDown('width', chart_size, safe_int(obj.get('width')), classId='dropdown_common dropdown_medium')]],
					[[XMLSpace(1), XMLSpan('Thresholds', classId='normal_text_title')], '', display_type_option],
					[[XMLSpace(1), XMLSpan('Unit', classId='normal_text_title')], [XMLDropDown('stat_unit', unit_options, safe_int(obj.get('stat_unit')), classId='dropdown_group_type')], [XMLSpace(1), XMLSpan('Occurance', classId='normal_text_title')], [XMLTextField('occurance', safe_int(obj.get('occurance')), classId='report_textfield_small')]],
					[[XMLSpace(1), XMLSpan('Red', classId='normal_text_title')], XMLTextField('red_val', safe_float(obj.get('red_val')), classId='textfield_popup textfield_small'), [XMLSpace(1), XMLSpan('Yellow', classId='normal_text_title')], XMLTextField('yellow_val', safe_float(obj.get('yellow_val')), classId='textfield_popup textfield_small')],
			]
		ret['namefilter'] = XMLTable(name_filter, classId='alignedTableForm')
		ret['reportoptions'] = XMLTable(report_options, classId='alignedTableForm')
		ret['title_heading'] = obj.get('name', '')
		request['ret'].update(ret)

	def saveAsNewReport(self, request):
		"""Save as New Report.
		"""
		# check name availability
		#According to the BUG the save adhoc report was modified Everest3.0 BUG 557
		userPref = request.get("__user","")
		rights = ""
		if userPref:
			rights = userPref.get("selectedRights","")
		if rights == "ReadOnly":
			try :
			    	return {"content":XMLContentTable(["You are not authorized to perform this action", BR, BR,BR, BR,
						XMLCloseButton("Close"),BR],table_heading = "Error")}
		    	except :
			    	return {"content":XMLContentTable(["You are not authorized to perform this action", BR, BR,BR, BR,
						XMLCloseButton("Close"),BR],table_heading = "Error")}
		#According to the BUG the save adhoc report was modified Everest3.0 BUG 557		
		self.checkStatCount()			
		title_str = 'Error in Saving Report'
		if request.get('name', '') == '':
			try:
				content = interText.interString['S1438']
			except:
				content = 'No Report Profile Name entered. Please enter valid Report Profile Name.'
			return self.formContent(title_str, content)
		# check in report table
		objs = self.report_profile.getReportObjs(filter_condn=["name = '%s'" %(request['name'])])
		if objs and objs != -1:
			try:
				content = interText.interString['S1453']
			except:
				content = 'Report Profile Name entered is already present. Please try another valid Report Profile Name.'
			return self.formContent(title_str, content)
		# check in report list table
		objs = self.report_profile.getReportListObjs(filter_condn=["name = '%s'" %(request['name'])])
		if objs and objs != -1:
			try:
				content = interText.interString['S1453']
			except:
				content = 'Report Profile Name entered is already present. Please try another valid Report Profile Name.'
			return self.formContent(title_str, content)
		view_type = safe_int(request.get('View_type'))
		# For Resource View
		if view_type == 0:
			root_path_key = 'root_path'
		# For Location View
		elif view_type == 1:
			root_path_key = 'loc_path'
		# For Domain View
		elif view_type == 2:
			root_path_key = 'dom_path'
		elif view_type == 3:
			root_path_key = 'biz_path'
		user_root_path = request.get('__user', {}).get(root_path_key, ROOT_PATH)
		# Form ReportObj
		report_obj = {}	
		#given fix for the Timescale selected and Saving he report --Everest 3.0 BUG509
		time_selection = safe_int(request.get('timescale',3))
		report_obj['timesel'] = time_selection
		if request.get('modify'):			
			request['t1_start_time'] = timeStrTotimeInt(request.get('start_time',0)+":00")
			request['t1_end_time'] = timeStrTotimeInt(request.get('end_time',0)+":00")			
			if request.get('cal_val'):
				report_obj['timesel'] = 3
		#given fix for the Timescale selected and Saving he report --Everest 3.0 BUG509
		
		report_obj_datas = ['name', 'category', 'resfilter', 'pathnames', 'resolution', 'business_hr', 'business_hr_profile', 'topn', 'timebetween','t1_start_time','t1_end_time']
		for data in report_obj_datas:
			report_obj[data] = request.get(data, '')
			if data == 'pathnames':
				result = ''
				val = request.get(data, '')
				if user_root_path.startswith('root/'):
					user_root_path = user_root_path[5:]
				val = val.strip()
				if val != '':
					if val.find(' | '):
						values = val.split(' | ')
						for value in values:
							if value.startswith(user_root_path) or value.startswith(user_root_path +str(PATH_SEP))  :
								result += value + ' | '
							else:
								result += user_root_path + PATH_SEP + value + ' | '
						if result.endswith(' | '):
							result = result[:-2]
					report_obj[data] = result
		report_obj['viewtype'] = safe_int(request.get('View_type'))		
		# default last hour for any report, if calendar or custom time option is being selected.			
		report_obj['user_list'] = 'Private'
		report_obj['private_opt'] = '%s , ' %(request.get('__user', {}).get('dn', ''))			
		new_report_id = self.report_profile.putReportObj(report_obj)
		# Form ReportListObj
		report_list_obj = {}
		report_list_obj_datas = ['name', 'stats', 'group_type', 'red_val', 'yellow_val', 'width', 'height']
		for data in report_list_obj_datas:
			report_list_obj[data] = request.get(data, '')
		red_val = safe_float(request.get('red_val', ''))
		yellow_val = safe_float(request.get('yellow_val', ''))
		report_list_obj['reportid'] = new_report_id
		report_list_obj['reporttype'] = request.get('report_type', '')
		report_list_obj['pval'] = iif(red_val >= yellow_val, 1, -1)
		report_list_obj['font'] = request.get('font', 's')
		report_list_obj['display'] = request.get('display', 'avg')
		group_id = self.report_profile.putReportListObj(report_list_obj)
		return {'content': XMLRedirect('selectgroup%s?reportdn=%d&group_id=%d' %(self.url_suffix, new_report_id, group_id))}

	def formContent(self, title_str, msg):
		content = [
					XMLMessage(msg), BR, BR, 
					XMLBackButton(interText.interButton['B001']), BR
				]
		return {
				'title' : title_str,
				'content': XMLContentTable(content, table_heading=title_str),
				'template': 'popuptemplate.htm'
			}

	def onNavigate(self, request):
		"""Navigation used in report.
		"""
		request['url'] = 'navigate'
		request['js'] = 'selectValueForReport'
		ret = self.selBrowse(request)
		ret['template'] = 'report_navigate.htm'
		return ret

	def getStatsList(self, request):
		"""Get all the statistics selected under the path.
		"""
		important_stats = []
		other_stats = []
		traps = []
		#rflogs = []
		#if request.get('report_type', '') not in ['traps', 'traps_summary', 'rflogs','nw_efficiency']:
		try:
			imp_stats = self.unms.initapi.apiIdMap['initapi'].reportAPIIdMap['reportapi'].important_stats.keys()
		except:
			imp_stats = []
		important_stats, other_stats = self.resource_manager.getStatisticsInMAX()
		if maxInputStats.DETAILED_NODE_INFO :
			important_stats += imp_stats
		#elif request.get('report_type', '') == 'rflogs':
		#rflogs = self.unms.res_ctrl.getRFDeviceParams(request)
		#else:
		trap_msg_profile = 'vsattrapmsg.cfg'
		if self.unms.resource_manager.profiles.has_key(trap_msg_profile):
			traps = map(lambda a: a.get('name', ''), self.unms.resource_manager.profiles[trap_msg_profile].getAllOutputStat())
		important_stats.sort()
		other_stats.sort()
		#rflogs.sort()
		traps.sort()
		stats = [[XMLSpan(interText.interString['S5233'], classId="title_text")]]
		for stat in important_stats:
			stats.append([XMLLink("javascript:selectStatForReport('%s');" %(stat), stat, classId='textpath')])
		if other_stats:
			stats.append([XMLSpan('Other Stats', classId='title_text')])
			for stat in other_stats:
				stats.append([XMLLink("javascript:selectStatForReport('%s');" %(stat), stat, classId='textpath')])
		if traps:
			stats.append([BR])
			stats.append([XMLSpan('VSAT Events', classId='title_text')])
			for stat in traps:
				stats.append([XMLLink("javascript:selectStatForReport('%s');" %(stat), stat, classId='textpath')])
		# if rflogs:
		# 	stats.append([BR])
		# 	stats.append([XMLSpan('RF Device Logs', classId='title_text')])
		# 	for stat in rflogs:
		# 		stats.append([XMLLink("javascript:selectStatForReport('%s');" %(stat), stat, classId='textpath')])
		return {'content': XMLTable(stats), 'template': 'report_navigate.htm'}

	def getResourceFilters(self, request):
		""" Get the resource filters required for reports
		"""
		ret = {}
		ret["template"] = "blanktemplate.htm"
		view_type = safe_int(request.get('View_type'))
		js_content = """
			function appendValue(val){
				//alert(1)
				//alert(val)
				var resfilterNewVal = $("#formatted").val();
				if(resfilterNewVal){
					resfilterNewVal = resfilterNewVal + val;
				}
				else {
					resfilterNewVal = val;
				} $("#formatted").val(resfilterNewVal);
			}
			function saveChanges(){div_obj=document.getElementById('resfilter_div');div_obj.innerHTML=$("#formatted").val();closeModalMsg();}
			function clearText(){$("#formatted").val("")}
		"""
		try:
			filter_options = [
				(' profile = ', interText.interField['F074']), (' poll_addr = ', interText.interString['S1927']),
				(' restype = ', interText.interString['S4388']), (' node_type = ', interText.interString['S3859']),
				(' device_name = ', interText.interString['S4431']), (' res_name = ', interText.interString['S4387']),
				(' alias ILIKE ', '%s %s' %(interText.interString['S4091'], interText.interString['S3199'])), (' node_alias ILIKE ', '%s %s' %(interText.interString['S3871'], interText.interString['S3199'])),
				(' descr = ', interText.interField['F323']), (' node_descr = ', interText.interField['F586']),
				(' src_addr = ', interText.interField['F161']), (' dest_addr = ', interText.interField['F166']),
				(' params LIKE ', 'Resource Parameters'),
			]
		except:
			filter_options = [
				(' profile = ', 'Poll Profile'), (' poll_addr = ', 'Polling Address'),
				(' restype = ', 'Resource Type'), (' node_type = ', 'Node Type'),
				(' device_name = ', 'Device Type'), (' res_name = ', 'Resource Name'),
				(' alias ILIKE ', 'Resource Alias'), (' node_alias ILIKE ', 'Node Alias'),
				(' descr = ', 'Resource Description'), (' node_descr = ', 'Node Description'),
				(' src_addr = ', 'Source Address'), (' dest_addr = ', 'Destination Address'),
				(' params LIKE ', 'Resource Parameters'),
			]
		# For Location View Filters
		if view_type == 1:
			try:
				filter_options.append((' l_name = ', interText.interField['F576']))
				filter_options.append((' l_descr ILIKE ', interText.interField['F579']))
			except:
				filter_options.append((' l_name = ', 'Location Name'))
				filter_options.append((' l_descr ILIKE ', 'Location Description'))
		# For Domain View Filters
		if view_type == 2:
			try:
				filter_options.append((' d_name = ', interText.interField['F580']))
				filter_options.append((' d_descr ILIKE ', interText.interField['F582']))
			except:
				filter_options.append((' d_name = ', 'Domain Name'))
				filter_options.append((' d_descr ILIKE ', 'Domain Description'))
		filters = []
		for option, display_name in filter_options:
			filters.append(XMLLink("javascript:appendValue('%s')" %(option), display_name, classId='commons1'))
		filters = makeChoppedList(filters, 2)
		table = [
					[XMLSpan('Filters', classId='normal_text_title')],
					[XMLTextArea('resfilter_new', request.get('resfilter', ''), id='formatted')],
					[BR],
					[XMLSpan('Filter Options', classId='normal_text_title')],
					[XMLTable(filters, classId="alignedTableForm")],
					[BR],
					[XMLSpan(' * Please enter the Filter condition within Double Quotes', classId='commons1')]
		]
		ret['content'] = [
			XMLTable(table, classId="alignedTableForm"), BR,
			XMLJS(js_content), XMLSpace(5), XMLJSButton('javascript:saveChanges();', 'Save'), XMLSpace(5),
			XMLJSButton('javascript:clearText();', 'Clear'), XMLSpace(5),
			XMLJSButton('javascript:closeModalMsg();', 'Close'),
		]
		return ret

	def getReportByCategory(self, request):
		""" Get list of reports by category
		"""
		category = request.get('category', '')
		user = request.get('__user').get('dn')
		# get the reports belong to this category
		reportTypeFilter = ""
		if not self.licserver.isFeatureEnabled('NUMBER_OF_NETFLOW_NODES') :
			reportTypeFilter = " and reporttype = 1"
		if user == 'administrator' or user == 'supermanager':
			filter_condn = ["category = '%s' %s" %(category, reportTypeFilter)]
		else:
			filter_condn = ["category = '%s' AND (user_list = 'Public' OR private_opt ILIKE '%%%s%%') %s" %(category, user, reportTypeFilter)]
		reports = self.report_profile.getReportObjs(filter_condn)
		return {'content': self.formReportByCategory(reports, request), 'template': 'blanktemplate.htm'}

	def formReportByCategory(self, reports, request):
		objs = []
		url_type_map = {1: 'report', 2: 'nfReport'}
		# format the data to display in proper manner
		for report in reports:
			reportid = safe_int(report.get('reportid'))
			name = ChangeName(report.get('name', ''))
			classId='textpath'
			if safe_int(request.get('reportdn')) == reportid:
				classId = 'textpath_selected'
			report_type = safe_int(report.get('reporttype'))
			content = XMLLink('%s?reportdn=%s' %(url_type_map.get(report_type, 'report'), reportid), name[:25], classId=classId,Title=name)
			objs.append(XMLDynamicDivTag("div_%s"%reportid, content, classId='report_div'))
		return objs

	def addToRealTime(self, request):
		try:
			resid = safe_int(request.get('resid'))
			statid = request.get('statids')
			# delete from real time monitoring
			self.real_time_server.deleteFromRealTimeHandler(resid)
			# add to real time monitoring
			self.real_time_server.addToRealTimeHandler(resid)
			return {'content': XMLRedirect('getrealtimereport?resid=%d&statids=%s'%(resid, statid)), 'template': 'popuptemplate.htm'}
		except Exception, msg:
			content = XMLContentTable([msg, BR, BR, XMLSubmitButton('javascript:window.close();', 'Close')], table_heading='Real Time Monitoring Error')
			return {'content': content, 'title': 'Real Time Report', 'template': 'popuptemplate.htm'}

	def getRealTimeReport(self, request):
		"""Get the real time report for the specified resid
		"""
		try:			
			request['realtime'] = 1
			self.checkStatCount()
			ret = {}
			report_dict = {}
			report_dict.update(request)
			report_dict['request'] = request
			template = 'popuptemplate.htm'
			if request.has_key('template'):
				template = request.get('template', '')
				if not template:
					template = 'popuptemplate.htm'
			report_dict['ret'] = {}
			report_dict['name'] = 'Real Time Report'
			self.getAdHocReportObj(report_dict)
			report_dict['report_obj']['timesel'] = 2
			out = self.basePublishObjs(report_dict)
			ret['title'] = 'Real Time Report'
			ret['content'] = [XMLJSLib('js/report.js'), out['content'], BR, BR, self.getRealTimeInfo(report_dict)]
			ret['template'] = template

			return ret
		except Exception, msg:
			msg = 'Exception while generating the report - %s' %(msg)
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report'), 'template': template}
		except NoReportException:
			msg = ChangeName('No Report is configured. Please configure some report')
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report'), 'template': template}
		except:
			msg = 'Exception while generating the report'
			logExceptionMsg(4, msg, self.module_name)
			return {'content': XMLContentTable([msg, BR, BR, XMLBackButton('Back')], table_heading='Error in Generating Report'), 'template': template}

	def getRealTimeInfo(self, report_dict):
		resid = safe_int(report_dict.get('resid'))
		statid = string.join(report_dict.get('statids'), ',')
		start_time = safe_int(self.real_time_server.realtime_info.get(resid, {}).get('startTime'))
		status = self.real_time_server.realtime_info.get(resid, {}).get('status', 1)
		real_time_duration = safe_int(maxInputStats.REAL_TIME_POLLING_DURATION) / 60
		table = [
					[XMLSpan('Real Time Monitoring will be done for maximum of %s minutes' %(`real_time_duration`))],
					[XMLSpan('Real Time Monitoring is started at %s' %(time.ctime(start_time)))],
				]
		reload_js = XMLJS('setTimeout("getRealTimeReport()", %d);' %(self.getRealTimeRefreshInterval()))
		if status == -1:
			end_time = safe_int(self.real_time_server.realtime_info.get(resid, {}).get('endTime'))
			table += [
					[XMLSpan('Real Time Monitoring is completed at %s.' %(time.ctime(end_time)))],
					[[XMLSpan('Please Click to '), XMLSpace(10), XMLLink('addtorealtimereport?resid=%d&statids=%s' %(resid, statid), 'Restart'), XMLSpace(5), XMLLink('javascript:window.close();', 'Close')]]
				]
			reload_js = None
		content = XMLTable(table, classId='alignedTableForm')
		if reload_js:
			content = [content, reload_js]
		return content

	def getRealTimeRefreshInterval(self):
		config = newReadAV('realtimeconfig.cfg')
		return safe_int(config.get('reportRefreshInterval', 30000))

	def getLeftPanel(self, report_dict):
		"""Get the left panel for report
		"""
		request = report_dict.get('request', {})
		report_id = safe_int(report_dict.get('reportdn'))
		request['report_obj'] = report_dict.get('report_obj')
		request['reportdn'] = report_id
		request['topn'] = safe_int(report_dict.get('topn', 4))
		report_types = getReportParams(report_dict).report_map.items()
		options = [
			('Time Scale', 'Timescale'), 
			#('', ['Space', 'Space', 'TimeBetween', 'Space', 'Space']),
			# ('Filter Options', ['Space','Space', 'ReportType', 'ReportTop', 'ReportResolution', 'Submit', 'ReportDefault', 'Space']),
			#('Filter Options', ['Space','Space', 'ReportGroupType', 'ReportTop', 'ReportResolution', 'Submit', 'ReportDefault', 'Space']),
		]
		# For Statistics report, no need of this option.
		# If it is a report profile one, then only add the reports.
		# For Statistics report, no need of this option.
		user_info = request.get('__user', {})
		if not (isPortalAccount(user_info) or isReadOnlyAccount(user_info)):
			typeOfReport = safe_int(report_dict.get('report_obj', {}).get('reporttype'))
			if (typeOfReport == 4):
				options.append(('Filter Options', ['Space', 'Space', 'ReportTop', 'DorSReport', 'SpecificReportType', 'Submit', 'ReportDefault', 'Space']))
				options.append(('Reports', 'ReportsByCategory'))
			else:
				if report_id != -1:
					options.append(('', ['Space', 'Space', 'TimeBetween', 'Space', 'Space']))
					options.append(('Filter Options', ['Space', 'Space', 'ReportType', 'ReportTop', 'ReportResolution', 'Submit', 'ReportDefault', 'Space']))
					options.append(('Reports', 'ReportsByCategory'))
				else:
					options.append(('Filter Options', ['Space', 'Space', 'ReportType', 'ReportResolution', 'Submit', 'Space']))
					report_types = filter(lambda a, b=self.threshold_based_charts: a[0] not in b, report_types)
		else:
			#options.append(('Filter Options', ['Space', 'Space', 'ReportType', 'Submit', 'Space', 'Space']))
			report_types = getReportParams(report_dict).cportal_report_map.items()
		request['time_durations'] = self.time_scale
		request['calendar_pos'] = (self.calendar_xpos, self.calendar_ypos)
		request['submit_url'] = 'javascript:submitPage();'
		request['top_options'] = getReportParams(report_dict).top_option
		request['resolutions'] = getReportParams(report_dict).resolution
		request['report_types'] = report_types
		request['report_group_types'] = getReportParams(request).report_group_map[:]
		request['default'] = safe_int(report_dict.get('report_obj', {}).get('isDefault'))
		if not request.has_key('timebetween'):
			request['timebetween'] = report_dict.get('report_obj', {}).get('timebetween','')
		reportTypeFilter = ""
		if not self.licserver.isFeatureEnabled('NUMBER_OF_NETFLOW_NODES') :
			reportTypeFilter = " AND reporttype = 1"
		user = request.get('__user').get('dn')
		if user == 'administrator' or user == 'supermanager':
			filter_condn = ["category = '%s'" %(report_dict['report_obj']['category'])]
		else:
			filter_condn = ["(user_list = 'Public' OR private_opt ILIKE '%%%s%%') AND category = '%s'" %(user, report_dict['report_obj']['category'])]
		report_objs = self.report_profile.getReportObjs(filter_condn)
		sql_stmt = "SELECT category FROM tblReports WHERE (user_list = 'Public' OR private_opt ILIKE '%%%s%%') %s GROUP BY category" %(user, reportTypeFilter)
		category_objs = DatabaseServer.executeSQLStatement(sql_stmt)
		request['category_objs'] = map(lambda a: a.get('category'), category_objs)
		request['report_objs'] = self.formReportByCategory(report_objs, request)
		hidden_vals, left_panel = self.left_panel.getLeftPanel(options, request)
		report_dict['ret']['hiddenFields'] = hidden_vals
		report_dict['ret']['left_panel'] = [left_panel, XMLJSLib('js/report.js')]

	def getImageLinks(self, report_dict):
		"""Construct the image link for the following options:
			1. Show All the Report profiles configured
			2. Edit the current report
			3. PDF Conversion
		"""
		report_id = safe_int(report_dict.get('reportdn'))
		image_links = []
		# Only for the report profile one.
		if report_id and report_id != -1:
			image_links = [
				XMLGifWithLabel('Report', 'reports_o.gif', 'Report'),				
			]
			#Added by neeraj priviliges for user account
			if report_dict.get('__user',{}).get('privileges',{}).get('ManageReport') :
				image_links.append(XMLGifWithLabel('editReport?select__%d&fromReport=1' %(report_id), 'images/edit.gif', 'Edit Report', usePost=1))

		user_info = report_dict.get('__user')
		disabled_menus = user_info.get('disabled_menus', '')
		if disabled_menus == None:
			disabledMenus = []
		else:
			disabledMenus = disabled_menus.split(':')
		'''	
		image_links.append(XMLGifWithLabel('javascript:getPDFCreator();', 'pdf.gif', 'PDF', flag=1),)
		image_links.append(XMLGifWithLabel('javascript:getCSVCreator();', 'csv.gif', 'Excel', flag=1),)
		image_links.append(XMLGifWithLabel('javascript:getWORDCreator();', 'word.gif', 'Word'),)
		'''
		if disabledMenus and 'Convert to PDF' not in disabledMenus:
			image_links.append(XMLGifWithLabel('javascript:getPDFCreator();', 'pdf.gif', 'PDF', flag=1),)
			image_links.append(XMLGifWithLabel('javascript:getHTMLCreator();', 'html.gif', 'HTML', flag=1),)
			image_links.append(XMLGifWithLabel('javascript:getCSVCreator();', 'csv.gif', 'CSV', flag=1),)
			image_links.append(XMLGifWithLabel('javascript:getXLSCreator();', 'excel.jpg', 'Excel', flag=1),)
			#word Icon removed since the word is also coming as the PDF the wordcreator.py need to reconstructed.
			#image_links.append(XMLGifWithLabel('javascript:getWORDCreator();', 'word.gif', 'Word'),)
		report_dict['ret']['ImageLink'] = XMLGifTable(image_links)

	def setDefault(self, request):
		"""Set Current Report as Default
		"""
		report_id = safe_int(request.get('reportdn'))
		default = safe_int(request.get('default'))
		default_report = 0
		if default:
			default_report = report_id
		# get the user preference obj and update the detault report field in that
		user_pref_obj = request['__usecure'].getUserPreferences(request['__user']['dn'])
		user_pref_obj['default_report'] = default_report
		request['__usecure'].putUserPreferences(user_pref_obj)
		return {'content': XMLRedirect('%s?reportdn=%d' %(self.url_suffix, report_id))}

	def getDefaultTimeScale(self, report_dict):
		"""Get the default Time scale option.
		Overwrite the base class method.
		"""
		time_scale = safe_int(report_dict.get('report_obj',{}).get('timesel', self.defaultTimeScale))
		if not time_scale:
			time_scale = self.defaultTimeScale
		return time_scale

	def getTimeFilters(self, request):
		"""Get the time filters from the user selected time range.
		Overwrite the base class method.
		"""
		init_option = 1
		business_hr = safe_int(request.get('report_obj', {}).get('business_hr','0'))
		time_scale = safe_int(request.get('timescale', ''))		
		cal_val = request.get('cal_val', '')
		#Added by sathish for Time1 
		custom_start_time = ''
		custom_end_time = ''
		if not request.has_key('start_time') and not request.has_key('timescale'):
			if request['report_obj'].get('t1_start_time', '') !=0 and request['report_obj'].get('t1_start_time', '') !='' and request['report_obj'].get('t1_start_time', '') != None:
				request['t1_start_time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(request['report_obj'].get('t1_start_time', '')))
				request['t1_end_time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(request['report_obj'].get('t1_end_time', '')))
		# modified by sathish for Time 1
		if request.has_key('start_time'):
			custom_start_time = request.get('start_time', '')	
		elif not request.has_key('timescale'):
			
			#request['t1_start_time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(request['report_obj'].get('t1_start_time', '')))
			custom_start_time = request.get('t1_start_time', '')
			
		
		if request.has_key('end_time'):
			custom_end_time = request.get('end_time', '')	
		elif not request.has_key('timescale'):
			
			#request['t1_end_time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(request['report_obj'].get('t1_end_time', '')))
			custom_end_time = request.get('t1_end_time', '')
		#end here
		
		request['resolution_changed'] = 0
		#For custom time
		if custom_start_time!='':
			init_option = 3
			
			start_time = custom_start_time + ':00'
			end_time = custom_end_time + ':59'
			st = safe_int(timeStrTotimeInt(start_time))
			et = safe_int(timeStrTotimeInt(end_time))
			request['st'] = st
			request['et'] = et
			if not st or not et:
				try:
					raise Exception, interText.interString['S5263']
				except:
					raise Exception, "Invalid Time Format. Please give the time as 'YYYY-MM-DD HH:MM'"
			#if not request.get('resolution'):
			resolution, high_resolution = self.getCustomTimeResolution(request)
			actual_resolution = resolution
			if request.get('resolution'):
				resolution = request.get('resolution')
				high_resolution = resolution
				# default resolution is change		
		
		#For duration
		if time_scale:
			init_option = 1
			selected = self.time_scale.get(time_scale, 1)
			start_time, end_time = eval(selected[1])()
			resolution = selected[2]
			actual_resolution = selected[2]
			high_resolution = selected[3]
			if request.get('resolution'):
				resolution = request.get('resolution')
				high_resolution = getReportParams(request).time_scale_resolution_map.get(time_scale, {}).get(resolution, resolution)
				# default resolution is changed
				request['resolution_changed'] = actual_resolution != resolution
		#For calendar
		elif cal_val:
			init_option = 2
			start_time, end_time, resolution, high_resolution = self.getCalendarTime(cal_val)
			actual_resolution = resolution
			if request.get('resolution'):
				resolution = request.get('resolution')
				high_resolution = getReportParams(request).time_scale_resolution_map.get(time_scale, {}).get(resolution, resolution)
				# default resolution is changed
				request['resolution_changed'] = actual_resolution != resolution
		#For custom time
		elif custom_start_time:
			init_option = 3
			start_time = custom_start_time + ':00'
			end_time = custom_end_time + ':59'
			st = safe_int(timeStrTotimeInt(start_time))
			et = safe_int(timeStrTotimeInt(end_time))
			request['st'] = st
			request['et'] = et
			if not st or not et:
				try:
					raise Exception, interText.interString['S5263']
				except:
					raise Exception, "Invalid Time Format. Please give the time as 'YYYY-MM-DD HH:MM'"
			#if not request.get('resolution'):
			resolution, high_resolution = self.getCustomTimeResolution(request)
			actual_resolution = resolution
			if request.get('resolution'):
				resolution = request.get('resolution')
				high_resolution = resolution
				# default resolution is changed
				request['resolution_changed'] = actual_resolution != resolution
		#Default time selection
		else:
			default_time_scale = self.getDefaultTimeScale(request)
			request['timescale'] = default_time_scale
			default_time = self.time_scale.get(default_time_scale)
			if default_time:
				start_time, end_time = eval(default_time[1])()
				resolution = default_time[2]
				high_resolution = default_time[3]
			else:
				start_time = 0
				end_time = 0
				resolution = ''
				high_resolution = ''
		request['st'] = safe_int(timeStrTotimeInt(start_time))
		request['et'] = safe_int(timeStrTotimeInt(end_time)) 
		# For Business Hour Report, If resolution is not selected by default, then for
		# Business/Non Business Hour Raw and Hour is Assigned
		if not request.get('resolution'): # No Resolution Selected
			if safe_int(business_hr) != 0 : # Selected Report is for Business Hour
				if time_scale : # For Default Reports
					if time_scale in [1, 2, 3] : # Report is Curr Hour/Last Hour/Last 3 Hour
						request['resolution'] = 'raw'
					else : # For Other Report
						request['resolution'] = 'hour'
				else : # Custom or Calender Report
					# If resolutiion is other than that of raw
					# Make it as a Hour Resolution
					if resolution == 'raw' :
						request['resolution'] = 'raw'
					else :
						request['resolution'] = 'hour'
			else :
				# Selected Report is for All Hour
				request['resolution'] = resolution
		high_resolution = self.getHighResolution(resolution, high_resolution)
		request['init_option'] = init_option
		request['high_resolution'] = high_resolution
		if maxInputStats.isSimulation == 1:
			start_time = safe_int(self.simulation_start_time)
			end_time = safe_int(self.simulation_end_time)
			request['st'] = start_time
			request['et'] = end_time
			request['resolution'] = 'raw'
			request['high_resolution'] = 'hour'

	def getCustomTimeResolution(self, request):
		""" Get the resolution for the custom time range.
		"""
		st_time = request.get('st')
		et_time = request.get('et')
		# less than or equal to an hour
		if (et_time - st_time) < 2 * 3600:
			resolution = 'raw'
			high_resolution = 'raw'
		# less than 3 hours
		elif (et_time - st_time) <= 3 * 3600:
			resolution = 'raw'
			high_resolution = 'hour'
		# less than or equal to one day
		elif (et_time - st_time) <= 2 * 86400:
			resolution = 'hour'
			high_resolution = 'hour'
		# less than 3 days
		elif (et_time - st_time) <= 3 * 86400:
			resolution = 'hour'
			high_resolution = 'day'
		# less than a month
		elif (et_time - st_time) <= 31 * 86400:
			resolution = 'day'
			high_resolution = 'day'
		# less than 2 month
		elif (et_time - st_time) <= 2 * 31 * 86400:
			resolution = 'day'
			high_resolution = 'month'
		# more than 2 month
		else:
			resolution = 'month'
			high_resolution = 'month'
		return resolution, high_resolution

	#==========================================
	# For Down Time Report
	#==========================================
	def filterAlarms(self, report_dict) :
		"""Get all Alarms and Events and construct the 
		Down Time Report for the configured resource
		"""
		logMsg(2,"Entered the function : filterAlarms", self.module_name)
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		# Update the report dict, making top = ''
		top = safe_int(report_dict.get('topn','0'))
		report_dict.update({"top" : "0"})
		# Get only resource Up/Down Events, Assuming Severity 6 is up/down Event
		events = filter(lambda a: safe_int(a.get("actual_severity","6")) == 6,self.getEvents(report_dict))
		if len(events) > 0:
			events = self.checkForBusinessHours(report_dict,events)

		report_dict.update({"top" : top})
		resids = report_dict.get('res_path_map', {}).keys()
		stats = report_dict.get('report_obj', {}).get('statids', [])
		# Get all the Alarms [Severity = Major and are Currently Up]
		stats = report_dict.get('report_obj', {}).get('statids', [])
		sql_stmt = 'SELECT resid, timeStamp as "timeStamp" from tblAlarms where thresid in (SELECT thresid from tblThresholds where resid in (%s) and severity = 6 and statid in (%s)) and isDeleted = 0'%(str(resids)[1:-1],str(stats)[1:-1])
		logMsg(2,"Get all the Alarms Severity = Major and are Currently Up", self.module_name)
		alarms = DatabaseServer.executeSQLStatement(sql_stmt)
		# Initializing filteredEvents, for the resource having alarms
		filteredEvents = {}
		for alarm in alarms:
			res_id = alarm.get("resid",None)
			if not filteredEvents.has_key(res_id):
				# If alarm occur before startTime
				if alarm.get("timeStamp") < start_time :
					filteredEvents[res_id] = {}
					filteredEvents[res_id]["numberOfEvents"]  = 1
					filteredEvents[res_id]["latestEventTime"]  = safe_int(alarm.get("timeStamp"))
					filteredEvents[res_id]["firstEventTime"]  = safe_int(alarm.get("timeStamp"))
					filteredEvents[res_id]["numberOfAlarms"]  = 1
					filteredEvents[res_id]["totalDownTime"] = end_time - start_time
					filteredEvents[res_id]["down_up_events"] = "Down :" + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(filteredEvents[res_id]["latestEventTime"])))  + ';<BR>' + "Up : - ;<BR> Time :" + str(makeTimeDelta(filteredEvents[res_id]["totalDownTime"]))  + ';<BR>'
		# Constructing eventObjDict, eventObjDict[resid] = total Events Corresponding to that resource
		# and initializing the filteredEvents for that resources
		eventObjDict = {}
		for event in events:
			res_id = event.get("resid",None)
			if not eventObjDict.has_key(res_id):
				eventObjDict[res_id] = [event]
				filteredEvents[res_id] = {}
			else:
				eventObjDict[res_id].append(event)

		for res_id in eventObjDict.keys():
			eventObjs = eventObjDict[res_id] # All Events for that resource
			numOfEvents = len(eventObjs)
			filteredEvents[res_id]["numberOfEvents"]  = numOfEvents
			filteredEvents[res_id]["latestEventTime"]  = safe_int(eventObjs[0].get("timeStamp"))
			filteredEvents[res_id]["firstEventTime"]  = safe_int(eventObjs[numOfEvents - 1].get("timeStamp"))
			#filteredEvents[res_id]["numberOfAlarms"]  = len(filter(lambda a, : safe_int(a.get("severity") == 6),eventObjs ))

			eventObjs.reverse()
			filteredObjs = []
			lastevent = "Unknown"
			
			# filteredObjs :  contain the events in up-down-up-down format
			for eventObj in eventObjs :
				isDownEvent = self.isDownEvent(eventObj)
				isUpEvent = not(isDownEvent)
				if lastevent == "Unknown" :
					if isDownEvent == 1 :
						lastevent = "Down"
					if isUpEvent == 1 :
						lastevent = "Up"
					filteredObjs.append(eventObj)
					continue
				if lastevent == "Up" and isUpEvent == 1 :
					continue

				if lastevent == "Down" and isDownEvent == 1 :
					continue
				if isDownEvent :
					lastevent = "Down"
				else :
					lastevent = "Up"
				filteredObjs.append(eventObj)

			eventObjs = filteredObjs
			eventObjs.reverse()
			numOfEvents = len(eventObjs)
			downTime = 0
			down_up_events = ""
			down_up_events1 = ""
			down_up_events2 = ""
			# Resource was down before the start time and came up in the timeframe
			# First Event is Resource Up Event
			if len(eventObjs) > 0:
				lastEvent = eventObjs[numOfEvents - 1]
				if self.isUpEvent(lastEvent):
					downTime = downTime + safe_int(lastEvent.get("timeStamp",0)) - start_time
					down_up_events1 = down_up_events1 + "Down :" + " - "  + ';<BR>' + "Up :"+ str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(safe_int(lastEvent.get("timeStamp",0))))) +";<BR> Time :" + str(makeTimeDelta(safe_int(lastEvent.get("timeStamp",0)) - start_time))  + ';<BR>' 
					eventObjs.remove(lastEvent)
			# Resource has gone down and not come up within the time frame
			# Last Event is Resource Down Event
			if len(eventObjs) > 0:
				firstEvent = eventObjs[0]
				if self.isDownEvent(firstEvent):
					if end_time < safe_int(time.time()) :
						downTime = downTime + end_time - safe_int(firstEvent.get("timeStamp",0))
						down_up_events2 =  "Down :" + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(safe_int(firstEvent.get("timeStamp",0)))))  + ';<BR>' + "Up : - ;<BR> Time :" + str(makeTimeDelta(end_time - safe_int(firstEvent.get("timeStamp",0))))  + ';<BR>' 
					else :
						downTime = downTime + safe_int(time.time()) - safe_int(firstEvent.get("timeStamp",0))
						down_up_events2 =  "Down :" + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(safe_int(firstEvent.get("timeStamp",0)))))  + ';<BR>' + "Up : - ;<BR> Time :" + str(makeTimeDelta(safe_int(time.time()) - safe_int(firstEvent.get("timeStamp",0))))  + ';<BR>'
					eventObjs.remove(firstEvent)
			timeDown = 0
			timeUp = 0
			
			# Calculating the Downtime
			for event in eventObjs:
				if self.isDownEvent(event):
					timeDown = safe_int(event.get("timeStamp",0))
				if self.isUpEvent(event):
					timeUp = safe_int(event.get("timeStamp",0))
				if timeDown != 0 and timeUp != 0:
					downTime = downTime + (timeUp - timeDown)
					down_events1 = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timeDown))) 
					up_events1 =  str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timeUp))) 
					down_up_events =  "Down: " + down_events1 + ';<BR>' + "Up: " + up_events1 + ';<BR>' + "Time :" + str(makeTimeDelta(timeUp - timeDown)) + ';<BR>' + down_up_events
					timeDown = 0
					timeUp = 0
			filteredEvents[res_id]["totalDownTime"] = downTime
			filteredEvents[res_id]["down_up_events"] = down_up_events1 + down_up_events + down_up_events2

		for event in events:
			try:
				res_id = event.get("resid","")
				if res_id == "":
					logMsg(2,"Resource Id is not present for the Event", self.module_name)
					continue
				# Event is already present in filteredevents update the same
				if filteredEvents.get(res_id,"") != "" :
					if safe_int(event.get("timeStamp",0)) < filteredEvents[res_id].get("firstEventTime",0) :
						if event.get("evt_type","") == "Resource Down":
							#filteredEvents[res_id]["numberOfAlarms"] = filteredEvents[res_id]["numberOfAlarms"] + 1
							filteredEvents[res_id]["totalDownTime"] = filteredEvents[res_id]["totalDownTime"] + filteredEvents[res_id]["firstEventTime"] - event.get("dtimeStamp",time.time())
						if event.get("evt_type","") == "Resource Up" or event.get("evt_type","") == "Resource Down":
							#filteredEvents[res_id]["numberOfEvents"] = filteredEvents[res_id]["numberOfEvents"] + 1
							filteredEvents[res_id]["firstEventTime"] = safe_int(event.get("timeStamp",0))
						else:
							logMsg(2,"Unknown Event for reports : %s"%(`event`), self.module_name)
						
					else:
						logMsg(3,"Unsorted Event list for reports", self.module_name)
				# Event is not present in filteredevents add the same 
				elif safe_int(event.get("severity","")) == 6 :
					filteredEvents[res_id]={}
					filteredEvents[res_id]["firstEventTime"] = safe_int(event.get("dtimeStamp",time.time()))
					filteredEvents[res_id]["latestEventTime"] = safe_int(event.get("dtimeStamp",time.time()))
					filteredEvents[res_id]["numberOfEvents"] = 1
					filteredEvents[res_id]["numberOfAlarms"] = 1
					filteredEvents[res_id]["totalDownTime"] = end_time - safe_int(event.get("dtimeStamp",time.time()))
					filteredEvents[res_id]["down_up_events"] = "Down :" + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))  + ';<BR>' + "Up : - ;<BR> Time :" + str(makeTimeDelta(filteredEvents[res_id]["totalDownTime"]))  + ';<BR>'
				elif safe_int(event.get("severity","")) == 1 :
					filteredEvents[res_id]={}
					filteredEvents[res_id]["firstEventTime"] = safe_int(event.get("dtimeStamp",time.time()))
					filteredEvents[res_id]["latestEventTime"] = safe_int(event.get("dtimeStamp",time.time()))
					filteredEvents[res_id]["totalDownTime"] = 0
				else:
					logMsg(2,"Unknown Event for reports : %s"%(event.get("evt_type","")), self.module_name)
					logMsg(2,"The event is : %s"%(`event`), self.module_name)


			except Exception,msg:
				logMsg(2,"Exception while filtering the Events for downTime report : %s,%s"%(Exception,msg), self.module_name)

		for res_id,event in filteredEvents.items():
			if safe_int(event.get("severity", "")) == 6 :
				filteredEvents[res_id]["totalDownTime"] = event["totalDownTime"] + event["firstEventTime"] - start_time
				filteredEvents[res_id]["down_up_events"] =  filteredEvents[res_id].get("down_up_events","") + "Down :" + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(event["firstEventTime"])))  + ';<BR>' + "Up : - ;<BR> Time :" + str(makeTimeDelta(filteredEvents[res_id]["totalDownTime"]))  + ';<BR>' 
				
		# Get all the Alarms [Severity = Major and are Currently Up]
		sql_stmt = 'SELECT resid,timeStamp as "timeStamp" from tblAlarms where thresid in (SELECT thresid from tblThresholds where resid in (%s) and severity = 6 and statid in (%s)) and isDeleted = 0'%(str(resids)[1:-1],str(stats)[1:-1])
		logMsg(2,"Get all the Alarms Severity = Major and are Currently Up", self.module_name)
		alarms = DatabaseServer.executeSQLStatement(sql_stmt)
		# Filter alarms based on the starting time. ie consider all alarms that 
		# were raised before startTime and were alive till the end_time
		for alarm in alarms:
			res_id = alarm.get("resid",None)
			if res_id == None :
				logMsg(2,"Resource Id doesn't exist for this Alarm", self.module_name)
				continue
			if safe_int(alarm.get("timeStamp",0)) < start_time and filteredEvents.get(alarm.get("res_id"),None) == None :
				filteredEvents[res_id]={}
				filteredEvents[res_id]["firstEventTime"] = safe_int(alarm.get("timeStamp",0))
				filteredEvents[res_id]["latestEventTime"] = safe_int(alarm.get("timeStamp",0))
				filteredEvents[res_id]["numberOfEvents"] = 1
				filteredEvents[res_id]["numberOfAlarms"] = 1
				filteredEvents[res_id]["totalDownTime"] = safe_int(time.time()) - safe_int(start_time)
				filteredEvents[res_id]["down_up_events"] = "Down :" + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(filteredEvents[res_id]["firstEventTime"])))  + ';<BR>' + "Up : - ;<BR> Time :" + str(makeTimeDelta(filteredEvents[res_id]["totalDownTime"]))  + ';<BR>'
		for res_id in filteredEvents.keys() :
			resObj = self.resource_manager.getObj(res_id)
			filteredEvents[res_id]["restype"] = resObj.get("restype","")
			filteredEvents[res_id]["PBS"] = resObj.get("PBS","")
		logMsg(2,"Exiting the function : filterAlarms", self.module_name)
		return filteredEvents
			
	def isDownEvent(self, eventObj) :
		"""Check whether the provided event is down
		Currently checking from severity = 6, Need to be changed
		for checking from the corresponding profiles
		"""
		if safe_int(eventObj.get("severity","6")) == 6 :
			return 1
		return 0

	def isUpEvent(self, eventObj) :
		"""Check whether the provided event is down
		Currently checking from severity = 6, Need to be changed
		for checking from the corresponding profiles
		"""
		if safe_int(eventObj.get("severity","1")) ==  1:
			return 1
		return 0

	def makeDownTimeTable(self, statRecords, report_dict):
		"""Make Down Time Table
		"""
		logMsg(2,"Entered function makeDownTimeTable", self.module_name)
		res_path_map = report_dict.get('res_path_map')
		try:
			headings = [('path', interText.interOption['O841'], '25%', 'left'), ('restype', interText.interOption['O006'], '15%', 'left'), ('down_up_events', interText.interString['S4089'], '35%', 'left'),('totalDownTime', interText.interString['S4090'], '25%', 'left')]
		except:
			headings = [ ('path', 'Resource', '25%', 'left'),('restype', 'Resource Type', '15%', 'left'),('down_up_events', 'Down Time', '35%', 'left'),('totalDownTime', 'Total Down Time', '25%', 'left')]
		#headings = [ ('path', 'Resource', '40%', 'left'),('restype', 'Resource Type', '10%', 'left'),('totalDownTime', 'Total Down Time', '15%', 'left'),('firstEventTime', 'First EventTime', '15%', 'left'),('latestEventTime', 'Last Event Time', '15%', 'left')]
		#headings = [ ('path', 'Resource', '25%', 'left'),('restype', 'Resource Type', '15%', 'left'),('down_up_events', 'Down Time', '35%', 'left'),('totalDownTime', 'Total Down Time', '25%', 'left')]
		objs = []
		for resid in statRecords.keys() :
			path = res_path_map.get(resid)
			statrecord = statRecords.get(resid,{})
			obj = {}
			obj['path'] = XMLSpan(makeChoppedPath(path))
			if not statrecord.get("down_up_events",'') :
				statrecord["down_up_events"] = '-'
			obj['down_up_events'] = XMLSpan(statrecord.get("down_up_events",'-'))
			obj['restype'] = XMLSpan(statrecord.get("restype",0))
			obj['totalDownTime'] = statrecord.get("totalDownTime",0)
			obj['firstEventTime'] = XMLSpan(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(statrecord.get("firstEventTime",0))), "")
			obj['latestEventTime'] = XMLSpan(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(statrecord.get("latestEventTime",0))), "")
			objs.append(obj)
		objs.sort(lambda a, b, c = [(-1, 'totalDownTime')] : cmpField(a, b, c))
		if safe_int(report_dict.get('top','')) :
			top =  safe_int(report_dict.get('top',''))
			objs = objs[0:top]
		map(lambda a: a.update({'totalDownTime' : makeTimeDelta(a.get('totalDownTime',0))}),objs)
		width = report_dict.get('width', SMALL_REPORT_WIDTH)
		if width > SMALL_REPORT_WIDTH:
			if len(objs) < 15:
				height = (len(objs) * 16) + 50
			else:
				height = 280
		else:
			height = 280
			if len(objs) < 15 and safe_int(report_dict.get("fromPDF",0)):
				self.addDummyObjs(headings, objs)
		logMsg(2,"Exiting function makeDownTimeTable", self.module_name)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width)

	def makeFolderSummary(self, report_dict) :
		"""Implemented One Line Summary Report
		"""
		logMsg(2,"Entered the function : makeFolderSummary", self.module_name)
		objs = []
		try:
			headings = [('folder', interText.interString['S1630'], '40%', 'left'), ('stat', interText.interString['S3072'], '10%', 'left'), ('avg', interText.interOption['O596'], '15%', 'left'), ('max', interText.interString['S5214'], '15%', 'left'), ('min', interText.interLink['L205'], '15%', 'left')]
		except:
			headings = [('folder', 'Folder', '40%', 'left'),('stat', 'Statistics', '20%', 'left'),('avg', 'Average', '15%', 'left'),('max', 'Maximum', '15%', 'left'),('min', 'Minimum', '15%', 'left')]
		actual_path_dict = report_dict["actual_path_dict"]
		stats = report_dict.get('report_obj', {}).get('statids', [])
		for path,path_filter in actual_path_dict.items() :
			logMsg(2,"Get all the resources under the folder", self.module_name)
			resourceObjs = self.getResourcesWithinFolder(report_dict, folder_summary=1, single_folder=path_filter)
			resids = map(lambda a: safe_int(a.get("resid","")),resourceObjs)
			logMsg(2,"Get all the data from statistics table", self.module_name)
			dataObjs = self.getDatasFromDB(report_dict, '', folder_summary = 1, selected_resids = resids)
			for stat in stats :
				obj = {}
				filter_data_objs = filter(lambda a:safe_int(a.get('statid')) == safe_int(stat),dataObjs)
				dataVals = map(lambda a: safe_float(a.get('avg')),filter_data_objs)				
				obj['folder'] = path
				obj['stat'] = changeOption(self.statid_name_map.get(safe_int(stat)))
				if len(dataVals) > 0 :
					obj['actual_avg_val'] = avg(dataVals)
					max_value = max(dataVals)
					division_factor, unit_prefix = self.formatUnit(max_value)
					unit = self.statid_unit_map.get(safe_int(stat))
					if unit in self.units_to_be_formatted :
						if unit == 'bps':
							temp_unit = unit_prefix + unit
						elif unit[:1] == '*':
							temp_unit = unit_prefix + unit[1:]
						else:
							temp_unit = unit
						obj.update({'avg' : '%.2f %s' %(avg(dataVals) / division_factor, temp_unit), 'max' : '%.2f %s' %(max(dataVals) / division_factor, temp_unit), 'min' : '%.2f %s' %(min(dataVals) / division_factor, temp_unit)})
					else:
						temp_unit = unit
						obj.update({'avg' : '%.2f %s' %(avg(dataVals) / division_factor, temp_unit), 'max' : '%.2f %s' %(max(dataVals) / division_factor, temp_unit), 'min' : '%.2f %s' %(min(dataVals) / division_factor, temp_unit)})
				else :	
					obj['actual_avg_val'] = -1
					obj.update({'avg' : -1, 'max' : -1, 'min' : -1})
				objs.append(obj)
		objs.sort(lambda a, b, c = [(-1, 'actual_avg_val')] : cmpField(a, b, c))
		# Done for Sorting
		for obj in objs :
			if obj.get('avg') == -1 :
				obj.update({'avg' : "_", 'max' : "_", 'min' : "_"})
			else :
				pass
		width = report_dict.get('width', SMALL_REPORT_WIDTH)
		if width > SMALL_REPORT_WIDTH:
			if len(objs) < 15:
				height = (len(objs) * 16) + 50
			else:
				height = 280
		else:
			height = 280
			if len(objs) < 15 and safe_int(report_dict.get("fromPDF",0)):
				self.addDummyObjs(headings, objs)
		logMsg(2,"Exiting the function : makeFolderSummary", self.module_name)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width)

	def makeVSATPingTable(self, params, request, isFormatChange=0):
		"""This is generating the VSAT Ping Report. This function has to just call Data table function
		and transpose the data to handle large data types.
		"""
		try:
			returndata =''
			returndata = self.makeDataTableForVSATPing(params, request, isFormatChange)
			logMsg(2,'returndata - %s'%(returndata),'makeDataTable')
			return returndata
		except Exception,msg:
			logExceptionMsg(4, "Exception in makeVSATPingTable : %s %s" %(str(Exception), msg), self.single_name)
			return ''
	def makeVSATNetworkEfficiencyTable(self, report_dict):
		headings = [
				('hostname', 'VSAT ID', '10%', 'left'),
				('address', 'Location', '10%', 'left'),
				('city', 'City', '10%', 'left'),
				('state', 'State', '10%', 'left'),
				('total_days', 'Total Days', '10%', 'left'),
				('_conn_time', 'Attendance Connection Time', '10%', 'left'),
				('_uptime', 'Total Uptime (sec)', '10%', 'left'),
				('_conn_time_hrs', 'Connection Time (hh:mm:ss)', '10%', 'left'),
			]
		csv_headings = [
				('hostname', 'VSAT ID', 25),
				('_address', 'Location', 40),
				('city', 'City', 20),
				('state', 'State', 20),
				('total_days', 'Total Days', 10),
				('_conn_time', 'Attendance Connection Time', 27),
				('_uptime', 'Total Uptime (sec)', 20),
				('_conn_time_hrs', 'Connection Time (hh:mm:ss)', 27),
			]
		resids = report_dict.get('res_path_map', {}).keys()
		res_query = report_dict.get('res_query')
		stats = []
		start_time = report_dict['st']
		end_time = report_dict['et']
		total_days = getTotalNoOfDays(start_time, end_time)
		# vsat uptime
		st_time = safe_int(time.time())
		all_outage_by_resid = self.doVSATCalculations(start_time, end_time, report_dict)
		et_time = safe_int(time.time())
		logMsg(2, 'Time taken to get the uptime - %s' %(et_time - st_time), self.module_name)
		# get the connection attendance
		st_time = safe_int(time.time())
		trap_count_obj = self.getTrapsCount(report_dict)
		et_time = safe_int(time.time())
		logMsg(2, 'Time taken to get the trap count - %s' %(et_time - st_time), self.module_name)
		# For Download / upload stats
		stats = [safe_int(self.rawdn_id_map.get('bytes_download')), safe_int(self.rawdn_id_map.get('bytes_upload'))]
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		unit_map = {-1: 'Bytes', 0:'Bytes', 1:'KBytes', 2:'MBytes', 3:'GBytes'}
		for stat in stats:
			stat_name = self.statid_name_map.get(stat)
			headings.append((stat, "%s (%s)"%(stat_name, unit_map.get(stat_unit)), '10%', 'left'))
			csv_headings.append((stat, '%s (%s)'%(stat_name, unit_map.get(stat_unit)), 20))
		headings.append(('_total', 'Total (%s)'%unit_map.get(stat_unit), '10%', 'left'))
		csv_headings.append(('_total', 'Total (%s)'%unit_map.get(stat_unit), 15))
		st_time = safe_int(time.time())
		node_objs = self.getNodeBasedSummaryObjs(report_dict, stats)
		et_time = safe_int(time.time())
		logMsg(2, 'Time taken to get the vsat traffic details - %s' %(et_time - st_time), self.module_name)
		objs = []
		for resid, obj in node_objs.items():
			calc_obj = all_outage_by_resid.get(resid, {})
			uptime = safe_float(calc_obj.get('uptime'))
			obj['uptime'] = uptime
			obj['_uptime'] = safe_int(calc_obj.get('uptime'))
			obj['_conn_time_hrs'] = formatHHMMSS(safe_int(calc_obj.get('uptime')))
			obj['total_days'] = total_days
			obj['_conn_time'] = safe_int(trap_count_obj.get(resid))
			total = safe_float(obj.get('total'))
			objs.append((total, obj))
		top = safe_int(report_dict.get('topn'))
		objs.sort(sortTuple1)
		objs.reverse()
		objs = map(lambda a: a[1], objs)
		if top:
			objs = objs[:top]
		width = report_dict.get('width', 420)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY:
			report_dict['no_resource'] = len(objs)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)
	
	def makeVSATNetworkStatisticTable(self, report_dict):
		headings = [('sno', 'S.No', '5%', 'left')]
		csv_headings = [('sno', 'S.No', 25)]
		group_type = safe_int(report_dict.get('group_type', 0))
		# individual vsat wise
		if group_type == 1:
			headings.append(('vsatid', 'VSAT ID', '10%', 'left'))
			csv_headings.append(('vsatid', 'VSAT ID', 10))
		# platform wise grouping
		elif group_type == 2:
			headings.append(('_platform', 'Platform', '10%', 'left'))
			csv_headings.append(('_platform', 'Platform', 10))
		# customer wise grouping
		else:
			headings += [ 
							('platform', 'Platform', '10%', 'left'),
							('customer_name', 'Customer Name', '10%', 'left'),
						]
			csv_headings += [
						('platform', 'Platform', 10),
						('customer_name', 'Customer Name', 30),
					]
		headings += [
				('nw_uptime1', 'Rem.VSAT NW Uptime 1 (%)', '25%', 'left'),
				('nw_uptime2', 'Rem.VSAT NW Uptime 2 (%)','25%','left'),
				('performance', 'Performance (%)', '20%', 'left'),
			]
		csv_headings += [
				('nw_uptime1', 'Rem.VSAT NW Uptime 1 (%)', 30),
				('nw_uptime2', 'Rem.VSAT NW Uptime 2 (%)',30),
				('performance', 'Performance (%)', 20),
			]
		objs = []
		t1_start_time = report_dict.get('t1_start_time', '')
		t1_end_time = report_dict.get('t1_end_time', '')
		t2_start_time = report_dict.get('t2_start_time', '')
		t2_end_time = report_dict.get('t2_end_time', '')
		if report_dict['request'].get('timebetween','') or report_dict.get('timebetween',"") or report_dict['report_obj'].get('timebetween',''):
			t1_start_duration = time.strftime('%d-%b-%Y', time.localtime(t1_start_time))
			t1_end_duration = time.strftime('%d-%b-%Y', time.localtime(t1_end_time))
			t2_start_duration = time.strftime('%d-%b-%Y', time.localtime(t2_start_time))
			t2_end_duration = time.strftime('%d-%b-%Y', time.localtime(t2_end_time))
		elif report_dict['report_obj'].get('timebetween',''):
			t1_start_duration = time.strftime('%d-%b-%Y', time.localtime(t1_start_time))
			t1_end_duration = time.strftime('%d-%b-%Y', time.localtime(t1_end_time))
			t2_start_duration = time.strftime('%d-%b-%Y', time.localtime(t2_start_time))
			t2_end_duration = time.strftime('%d-%b-%Y', time.localtime(t2_end_time))
		else:
			t1_start_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(t1_start_time))
			t1_end_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(t1_end_time))
			t2_start_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(t2_start_time))
			t2_end_duration = time.strftime('%d-%b-%Y %H:%M', time.localtime(t2_end_time))
		# For Additional Time headings. Used in Excel
		additional_headings = []
		additional_headings.append('Time 1: %s - %s (%s)' %(t1_start_duration, t1_end_duration, getTimeZone()))
		additional_headings.append('Time 2: %s - %s (%s)' %(t2_start_duration, t2_end_duration, getTimeZone()))
		st_time = safe_int(time.time())
		report_dict['t1_all_outage_by_resid'] = self.doVSATCalculations(t1_start_time, t1_end_time, report_dict)
		et_time = safe_int(time.time())
		logMsg(2, 'Time taken to do the VSAT downtime calculation for the time - %s & %s is %d' %(t1_start_time, t1_end_time, (et_time-st_time)), self.module_name)
		st_time = safe_int(time.time())
		report_dict['t2_all_outage_by_resid'] = self.doVSATCalculations(t2_start_time, t2_end_time, report_dict)
		et_time = safe_int(time.time())
		logMsg(2, 'Time taken to do the VSAT downtime calculation for the time - %s & %s is %d' %(t2_start_time, t2_end_time, (et_time-st_time)), self.module_name)
		# individual vsat performance
		if group_type == 1:
			st_time = safe_int(time.time())
			objs = self.getVSATWiseAggregateObjs(report_dict)
			et_time = safe_int(time.time())
			logMsg(2, 'Total time taken to get the VSAT wise aggregate data - %s is %d' %(len(objs), (et_time-st_time)), self.module_name)
		# customer wise grouping
		elif group_type == 0:
			st_time = safe_int(time.time())
			objs = self.getCustomerWiseAggregateObjs(report_dict)
			et_time = safe_int(time.time())
			logMsg(2, 'Total time taken to get the Customer wise aggregate data - %s is %d' %(len(objs), (et_time-st_time)), self.module_name)
		# platform wise grouping
		else:
			st_time = safe_int(time.time())
			objs = self.getPlatformWiseAggregateObjs(report_dict)
			et_time = safe_int(time.time())
			logMsg(2, 'Total time taken to get the Platform wise aggregate data - %s is %d' %(len(objs), (et_time-st_time)), self.module_name)
		width = report_dict.get('width', 420)
		for obj in objs:
			obj['performance'] = '%.3f'%safe_float(obj.get('performance'))
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY :
			report_dict['no_resource'] = len(objs)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings, additional_headings=additional_headings)
	
	def makeUptimeSummaryTable(self, report_dict):
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		unit_map = {-1: '', 0:'', 1:'K', 2:'M', 3:'G', 4:'T'}
		headings = [('sno', 'S.No', '5%', 'left')]
		csv_headings = [('sno', 'S.No', 25)]
		group_type = safe_int(report_dict.get('group_type', ''))
		if safe_int(group_type) == 1:
			# customer information is selected. so need to display VSAT IDs.
			headings.append(('vsatid', 'VSAT ID', '10%', 'left'))
			csv_headings.append(('vsatid', 'VSAT ID', 10))
		elif group_type == 2:
			headings += [	('_platform', 'Platform', '10%', 'left'),
					('no_of_vsats', 'No. of VSATs', '10%', 'left'),
					('purchased_bandwidth', 'Purchased Bandwidth (Kbps)','10%','left'),
				]
			csv_headings += [('_platform', 'Platform', 15),
					('no_of_vsats', 'No. of VSATs', 12),
					('purchased_bandwidth', 'Purchased Bandwidth (Kbps)', 27),
			]
		else:
			headings += [ 
							('platform', 'Platform', '10%', 'left'),
							('customer_name', 'Customer Name', '10%', 'left'),
							('no_of_vsats', 'No. of VSATs', '10%', 'left'),
							('purchased_bandwidth', 'Purchased Bandwidth (Kbps)','10%','left'),
						]
			csv_headings += [
						('platform', 'Platform', 10),
						('customer_name', 'Customer Name', 30),
						('no_of_vsats', 'No. of VSATs', 12),
						('purchased_bandwidth', 'Purchased Bandwidth (Kbps)', 27),
					]
		# Selected Statistics
		stat_headings, stat_csv_headings = self.getHeadingsForStatistics(report_dict)
		headings += stat_headings
		csv_headings += stat_csv_headings
		# uptime / performance headings
		headings += [('_uptime', 'Total Uptime Duration', '10%', 'left'),
					('_total_duration', 'Total Duration', '10%', 'left'),
					('link_performance', 'Link Performance (%)', '10%', 'left'),
					('uptime_vsat_in_days', 'Average uptime /VSAT (Days)', '10%', 'left'),
				]
		csv_headings += [('_uptime', 'Total Uptime Duration', 25),
					    ('_total_duration', 'Total Duration', 15),
					    ('link_performance', 'Link Performance (%)', 25),
					    ('uptime_vsat_in_days', 'Average uptime/VSAT (Days)', 30),
				]
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		# Get the statistics values
		report_dict['stat_values'] = self.getVSATStatObjs(report_dict, aggregation=report_dict.get('aggregation', 'sum'))
		# get the downtime / uptime values
		report_dict['all_outage_by_resid'] = self.doVSATCalculations(start_time, end_time, report_dict)
		# get group based resources
		objs = []
		if safe_int(group_type) == 1:
			objs = self.getVSATWiseSummaryData(report_dict)
		elif group_type == 2:
			objs = self.getPlatformWiseSummaryData(report_dict)
		else:
			objs = self.getCustomerWiseSummaryData(report_dict)
		if len(objs) > 1:
			#To calculate the column wise total
			total_obj = self.getColumnWiseTotalSummaryData(objs, report_dict)
			objs.append(total_obj)
		width = report_dict.get('width', 420)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY :
			report_dict['no_resource'] = len(objs)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)
	
	def makeTrapsSummaryTable(self, events, report_dict):
		headings = [
				('hostname', 'VSAT ID', '10%', 'left'),
				('poll_addr', 'IP Address', '10%', 'left'),
				('subnet', 'Subnet Mask', '10%', 'left'),
				('address', 'Address', '10%', 'left'),#<Robinson Panicker 20120515> : use address key for displaying on HTML with <BR>
				('conperson', 'Contact Person', '10%', 'left'),
				('contact_no', 'Contact Number', '10%', 'left'),
				('city', 'City', '10%', 'left'),
				('state', 'State', '10%', 'left'),
				('occurance', 'No. of Events', '10%', 'left'),
			]
		csv_headings = [
				('hostname', 'VSAT ID', 15),
				('poll_addr', 'IP Address', 15),
				('subnet', 'Subnet Mask', 15),
				('_address', 'Address', 40),#<Robinson Panicker 20120515> : use _address key for displaying in Excel Link generated
				('conperson', 'Contact Person', 25),
				('contact_no', 'Contact Number', 20),
				('city', 'City', 20),
				('state', 'State', 20),
				('occurance', 'No. of Events', 15),
			]
		objs = []
		for event in events:
			if event.get('syslocation', '').find(' - ') != -1:
				try:
					locations = string.split(event.get('syslocation', ''), ' - ')
					if locations and len(locations) > 1:
						event['state'] = locations[0]
						event['city'] = string.join(locations[1:], ' - ')
				except Exception, msg:
					logMsg(2, 'Exception - %s - %s' %(`event`, msg), self.module_name)
			event['address'] = event.get('address', '').replace('"', '').replace("'", '').strip()
			if event.get('address', '').find(' ') != -1:
				event['address'] = event.get('address', '').replace(',', ', ').replace('  ', ' ')
			#<Robinson Panicker 20120515> : update _address key for displaying without HTML tags in excel
			event['_address'] = event.get('address', '')
			if not report_dict.has_key('fromPDF'):
				#<Robinson Panicker 20120515> : update address key for displaying on HTML with <BR>
				event['address'] = makeChoppedPath(event.get('address', ''), 30)
			param_map = updateParams(event.get('params', ''))
			event.update(param_map)
			objs.append(event)
		#To calculate the column wise total
		if objs:
			total_occurance = sum(map(lambda a : safe_int(a.get("occurance")), objs))
			total_obj = {'hostname': ' ', 
					'poll_addr': ' ' ,
					'subnet': ' ', 
					'address': ' ',
					'conperson':' ', 
					'contact_no': ' ',
					'city': ' ',
					'state': 'Total',
					'occurance': total_occurance,
				}
			objs.append(total_obj)
		width = report_dict.get('width', 420)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY :
			report_dict['no_resource'] = len(objs)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)
	
	def makeTrapsTable(self, events, report_dict):
		severityMap = {
				0: 'images/alarm_indicator_1.gif', 1: 'images/alarm_indicator_2.gif', 
				2: 'images/alarm_indicator_2.gif', 3: 'images/alarm_indicator_3.gif', 
				4: 'images/alarm_indicator_3.gif', 5 : 'images/alarm_indicator_4.gif', 
				6: 'images/alarm_indicator_5.gif',
			}
		res_path_map = report_dict.get('res_path_map')
		headings = [
				('_severity', 'Severity', '10%', 'left'), 
				('trapid', 'Event ID', '10%', 'left'), 
				('event_date', 'Date', '10%', 'left'), 
				('event_time', 'Time', '10%', 'left'), 
				('event_code', 'Event Name', '15%', 'left'), 
				('category', 'Element Name', '15%', 'left'), 
				('src_addr', 'IP Address', '15%', 'left'), 
				('eventid', 'Event Code', '10%', 'left'), 
				('_msg', 'Event Description', '30%', 'left'),
			]
		csv_headings = [
				('severity', 'Severity', 25), 
				('trapid', 'Event ID', 10), 
				('_date', 'Date', 10), 
				('_time', 'Time', 10), 
				('event_code', 'Event Name', 15), 
				('category', 'Element Name', 15), 
				('src_addr', 'IP Address', 15), 
				('eventid', 'Event Code', 10), 
				('msg', 'Event Description', 40),
			]
		
		objs = []
		event_codes = newReadAV(os.getcwd() + '/vsatconf/trapcode.ini')
		logMsg(2, "event_codes = %s , %s"%(os.getcwd() + '/vsatconf/trapcode.ini', event_codes), self.module_name)
		required_images = []
		for event in events:
			msg = event.get('msg', '')
			# For HTML Online Report
			severity_image = severityMap.get(safe_int(event.get('severity')), 1)
			severity_image = severity_image.replace('/', '\\\\')
			event['_severity'] = XMLImage(severity_image)
			# For HTML Offline Report
			if severity_image not in required_images:
				required_images.append(severity_image)
			event['_msg'] = XMLNOBR(event.get('msg'))
			#event['_timeStamp'] = XMLNOBR(time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(safe_int(event.get('timeStamp')))))
			lt = time.localtime(safe_int(event.get('timeStamp', event.get('timestamp'))))
			# For HTML Report
			event['event_date'] = time.strftime('%m/%d/%Y', lt)
			event['event_time'] = time.strftime('%H:%M:%S', lt)
			# For Excel Report
			if safe_int(report_dict.get('format_csv')) == 1:
				event['_date'] = DateTime.DateTime(lt[0], lt[1], lt[2])
				event['_time'] = DateTime.DateTime(lt[0], lt[1], lt[2], lt[3], lt[4], lt[5])
			else:
				event['_date'] = time.strftime('%m/%d/%Y', lt)
				event['_time'] = time.strftime('%H:%M:%S', lt)
			event['event_code'] = event_codes.get(str(event.get('eventid')), '')
			objs.append(event)
		# For HTML Offline Report
		required_images = unique(required_images)
		if required_images and safe_int(report_dict.get('html_offline_report')):
			if not report_dict.has_key('created_charts'):
				report_dict['created_charts'] = []
			report_dict['created_charts'].extend(required_images)
		width = report_dict.get('width', 420)
		if width > 420:
			if len(objs) < 15:
				height = (len(objs) * 16) + 50
			else:
				height = 280
		else:
			height = 280
			if len(objs) < 15:
				self.addDummyObjs(headings, objs)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY :
			report_dict['no_resource'] = len(objs)
			#headings = csv_headings
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)

	def getNodeBasedSummaryObjs(self, report_dict, stats):
		res_query = report_dict.get('res_query')
		resolution = report_dict.get('resolution', '')
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		top = safe_int(report_dict.get('topn'))
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		#<Robinson Panicker : 20121022> for vsat traffic report the conversion should be 1kbps = 1000bps[here we use this] and for data storage 1Kbytes = 1024 bytes
		if report_dict.has_key('vsattrafficreport'):
			if stat_unit == -1 :
				stat_unit = 0
			division_factor = math.pow(1000, stat_unit)
		else:
			division_factor = math.pow(1024, stat_unit)
		#<Robinson Panicker : 20121022> for vsat traffic report ends here
		estimated_report_len = 0
		time_filter = timeBetween(start_time, end_time, report_dict.get('timebetween',''))
		tables = self.db_map.get(resolution).getTables(start_time, end_time)
		sql_stmt = "SELECT r.*, n.*, en.region, en.conperson, en.contact_no, en.address FROM tblResConfig r, tblNodeInfo n, tblExtraNodeInfo en, (%s) p WHERE r.resid = p.resid AND r.poll_addr = n.poll_addr AND n.nodeid = en.nodeid AND r.poller_id = n.poller_id AND r.isDeleted = 0" %(res_query)
		objs = DatabaseServer.executeSQLStatement(sql_stmt)
		logMsg(4, "Node Summary Details Query %s"%sql_stmt, self.module_name)
		if report_dict.has_key('report_type_temp') and report_dict.get('report_type_temp', '') == 'nodes_summary' and (report_dict.get('topn', '') == '' or report_dict.get('topn', '') == 0) and objs:
			#The imformation in the upper query is used as an information for mapping in the query which is given below
			#The split query over here will work efficiently until the no of resources manged by the everest is <= 65000
			# so we will check how many query to execute at a time
			#no_of_query_to_execute = safe_int(math.floor(65000 / safe_float(len(objs))))
			#no_of_query_to_execute = safe_int(math.floor(6500 / safe_float(len(objs))))
			no_of_query_to_execute = safe_int(math.floor(maxInputStats.MAX_RECORDS_ONE_SHOT / safe_float(len(objs))))
			#no_of_query_to_execute = 20
			# condition to check whether to go for the split query or not
			# It will depend on the length of objs * length of tables 
			# If it is greater than 65000 then we will go for the Split query technique
			estimated_report_len = safe_int(len(objs) * len(tables))
		if safe_int(estimated_report_len) > safe_int(maxInputStats.MAX_RECORDS_FOR_REPORTS_DISPLAY):
			return {"maxLimitReached": 1}
		query_collection_all = []
		node_objs = {}
		map(lambda a, b=node_objs: b.update({safe_int(a.get('resid')): a}), objs)
		# Added by roopesh for the quering the data at one shot
		all_objs = []
		sql_stmt_list = []
		for table in tables:
			if resolution == 'raw':
				sql_stmt = "SELECT s.resid, s.statid, SUM(s.avg) total FROM %s s, (%s) r WHERE s.timeStamp BETWEEN %d AND %d %s AND s.resid = r.resid AND s.statid IN (%s) GROUP BY s.resid, s.statid"\
						 %(table, res_query, start_time, end_time, time_filter, str(stats)[1:-1])
			else:
				sql_stmt = "SELECT s.resid, s.statid, SUM(s.avg*s.count) total FROM %s s, (%s) r WHERE s.timeStamp BETWEEN %d AND %d %s AND s.resid = r.resid AND s.statid IN (%s) GROUP BY s.resid, s.statid" \
						 %(table, res_query, start_time, end_time, time_filter, str(stats)[1:-1])
			logMsg(2, "$$$$ sql_stmt -- %s"%sql_stmt, self.module_name) 
			sql_stmt_list.append(sql_stmt)
			if estimated_report_len > maxInputStats.MAX_RECORDS_FOR_SPLIT_QUERY and report_dict.has_key('report_type_temp') and report_dict.get('report_type_temp', '') == 'nodes_summary' and (report_dict.get('topn', '') == '' or report_dict.get('topn', '') == 0):
				query_collection_all.append(sql_stmt)
			#else:
				#objs = DatabaseServer.executeSQLStatement(sql_stmt)
				#if objs and objs != -1:
					#all_objs += objs
		if sql_stmt_list and not query_collection_all:
			try:
				sql_stmt_all = string.join(sql_stmt_list, ' UNION ALL ')
				sql_stmt = "SELECT a.resid, a.statid, SUM(total) total from (" + sql_stmt_all + ") as a GROUP BY a.resid, a.statid"				
				all_objs = DatabaseServer.executeSQLStatement(sql_stmt)
			except:
				logMsg(2,"Exception in Quering getNodeBasedSummaryObjs", self.module_name)
				all_objs = []
		# Added by roopesh for the quering the data at one shot ends here
		# calling the getSplitQueryTrafficReport() for generating the report in the pdfcreater.py
		if query_collection_all:
			ret = {}
			self.largeReportInProgress = 1
			titleText = report_dict['report_obj'].get('name', '')
			if report_dict.get('time_period_heading', '') != '':
				titleText = str(titleText) +  " for the Period " + str(report_dict.get('time_period_heading', ''))
			titleTab = report_dict['report_obj'].get('name', '')
			report_link = self.unms.pdf_creator.getSplitQueryTrafficReport(no_of_query_to_execute, query_collection_all, stats, self.statid_unit_map, report_dict, self.statid_name_map, titleText, titleTab)
			self.largeReportInProgress = 0
			return {'report_link' : report_link , 'title' : titleTab, 'isSplitQuery' : 1}
			#return ret
		classify_objs = classifyObjs(all_objs, lambda a: (safe_int(a.get('resid')), safe_int(a.get('statid'))))		
		for ((resid, statid), objs) in classify_objs.items():
			if node_objs.has_key(resid):
				stat_name = self.statid_name_map.get(statid)
				total = reduce(lambda a, b: a + b, map(lambda a: safe_float(a.get('total')), objs))
				node_objs[resid][statid] = total
		for resid, obj in node_objs.items():
			obj['address'] = obj.get('address', '').replace('"', '').replace("'", '')
			obj['_address'] = obj.get('address', '')
			if obj.get('syslocation', '').find(' - ') != -1:
				try:
					locations = string.split(obj.get('syslocation', ''), ' - ')
					if locations and len(locations) > 1:
						obj['state'] = locations[0]
						obj['city'] = string.join(locations[1:], ' - ')
				except Exception, msg:
					logMsg(2, 'Exception - %s - %s' %(`obj`, msg), self.module_name)
				if not report_dict.has_key('fromPDF'):
					obj['address'] = makeChoppedPath(obj.get('address', ''), 30)
			param_map = updateParams(obj.get('params', ''))
			obj.update(param_map)
			if obj.get('ex_type', ''):
				obj['portal_display_name'] = obj['ex_type']
			total = 0
			for stat in stats:
				stat_value = obj.get(stat)
				total += safe_float(stat_value)
				unit = self.statid_unit_map.get(stat)
				if stat_value == None:
					obj[stat] = '0'#'N/A' Asked by Airtel to have Zero value Instead for N/A
				else:
					obj[stat] = "%.3f"%(stat_value / division_factor)
			obj['total'] = total
			obj['_total'] = "%.3f"%(total / division_factor)
		del all_objs
		return node_objs
		
	def makeNodeBasedSummaryTable(self, report_dict):
		logMsg(2,"Entered the function : makeFolderSummary", self.module_name)
		objs = []
		headings = [
				('hostname', 'VSAT ID', '10%', 'left'),
				('portal_display_name', 'Portal Display Name', '10%', 'left'),
				#('sysContact', 'Customer_Name', '10%', 'left'),
				('poll_addr', 'IP_Address', '10%', 'left'),
				('subnet', 'Subnet_Mask', '10%', 'left'),
				('address', 'Address', '10%', 'left'),
				('conperson', 'Contact_Person', '10%', 'left'),
				('contact_no', 'Contact_Number', '10%', 'left'),
				('city', 'City', '10%', 'left'),
				('state', 'State', '10%', 'left'),
			]
		csv_headings = [
				('hostname', 'VSAT ID', 25),
				('portal_display_name', 'Portal Display Name', 25),
				('poll_addr', 'IP_Address', 15),
				('subnet', 'Subnet_Mask', 15),
				('_address', 'Address', 40),
				('conperson', 'Contact_Person', 25),
				('contact_no', 'Contact_Number', 20),
				('city', 'City', 20),
				('state', 'State', 20),
			]
		stat_unit = report_dict.get('report_obj', {}).get('stat_unit',1)
		division_factor = math.pow(1024, stat_unit)
		unit_map = {-1: 'Bytes', 0:'Bytes', 1:'KBytes', 2:'MBytes', 3:'GBytes'}
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		for stat in stats:
			stat_name = self.statid_name_map.get(stat)
			headings.append((stat, "%s (%s)"%(stat_name, unit_map.get(stat_unit)), '10%', 'left'))
			csv_headings.append((stat, '%s (%s)'%(stat_name, unit_map.get(stat_unit)), 20))
		headings.append(('_total', 'Total (%s)'%unit_map.get(stat_unit), '10%', 'left'))
		csv_headings.append(('_total', 'Total (%s)'%unit_map.get(stat_unit), 20))
		node_objs = self.getNodeBasedSummaryObjs(report_dict, stats)
		if type(node_objs) == types.DictType and node_objs.has_key('maxLimitReached') and node_objs.get('maxLimitReached'):
			return node_objs		
		if type(node_objs) == types.DictType and node_objs.has_key('isSplitQuery') and node_objs.get('isSplitQuery'):
			return node_objs
		top = safe_int(report_dict.get('topn'))
		objs = map(lambda a: (a.get('total'), a), node_objs.values())
		objs.sort(sortTuple1)
		objs.reverse()
		objs = map(lambda a: a[1], objs)
		if top:
			objs = objs[:top]
		width = report_dict.get('width', 420)
		if width > 420:
			if len(objs) < 15:
				height = (len(objs) * 16) + 50
			else:
				height = 280
		else:
			height = 280
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY :
			report_dict['no_resource'] = len(objs)
		logMsg(2,"Exiting the function : makeFolderSummary", self.module_name)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)

	def makeBackhaulLinkUtilTable(self, report_dict):
		headings = [('sno', 'S.No', '5%', 'left'), ('interface_name', 'Interface Name', '20%', 'left'),]
		csv_headings = [('sno', 'S.No', 25), ('interface_name', 'Interface Name', 75),]
		# Selected Statistics
		stat_headings, stat_csv_headings = self.getHeadingsForStatistics(report_dict)
		#logMsg(2, "stat_headings  -- %s           \n\nstat_csv_headings  --  %s"%(stat_headings, stat_csv_headings), self.module_name)
		vsat_affected_calc = safe_int(report_dict.get('vsat_affected_calc'))
		headings += stat_headings
		csv_headings += stat_csv_headings
		headings +=  [('link_down', 'Link Down Since', '10%', 'left'),
					('link_up', 'Link Up At', '10%', 'left'),
					('outage', 'Duration of Outage', '10%', 'left'),
				]
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		if (end_time - start_time) > 86399:
			csv_headings += [('link_down', 'Link Down Since', 17),
							('link_up', 'Link Up At', 17),
							('outage', 'Duration of Outage', 20),
					]
		else:
			csv_headings += [('link_down_hhmmss', 'Link Down Since', 17),
							('link_up_hhmmss', 'Link Up At', 17),
							('outage', 'Duration of Outage', 20),
					]
		if vsat_affected_calc:
			headings.extend([('vsats_affected', 'No. of VSATs affected', '10%', 'left')])
			csv_headings.extend([('vsats_affected', 'No. of VSATs affected', 20)])
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		if not safe_int(report_dict.get('topn')):
			# checking the no of records to be queried with the resid, statid combination.
			# if the record exceeds the number configured MAX_RECORDS_FOR_SPLIT_QUERY in maxInputStats
			# then split query technique will be used to generate the large chunk of data
			countStats, split_query_list, tables, high_resolution_tables, event_tables = self.getBackhaulStatObjsLength(report_dict)
			if safe_int(countStats) > safe_int(maxInputStats.MAX_RECORDS_FOR_REPORTS_DISPLAY):
				return {"maxLimitReached": 1}
			# checking if the countStats is greater than the MAX_RECORDS_FOR_SPLIT_QUERY in maxInputStats
			if countStats > maxInputStats.MAX_RECORDS_FOR_SPLIT_QUERY and split_query_list:
				logMsg(2, "Call makeSplitQueryReport...", self.module_name)
				self.largeReportInProgress = 1
				link, filePath = self.makeSplitQueryReport(split_query_list, report_dict, tables, high_resolution_tables, event_tables, csv_headings)
				self.largeReportInProgress = 0
				return {"isSplitQuery": 1, "report_link":link, "file_link":filePath}
			
		
		# Get the statistics values
		report_dict['stat_values'] = self.getBackhaulStatObjs(report_dict, aggregation=report_dict.get('aggregation', 'sum'))
		#logMsg(2, "report_dict['stat_values'] --- %s"%report_dict.get('stat_values', {}), self.module_name)
		# do downtime calculation
		resids = report_dict.get('data_resids', [])
		report_dict['all_outage_by_resid'] = self.doBackhaulDownTimeCalculations(report_dict)
		objs = self.getBackhaulAggregatedData(report_dict)
		width = report_dict.get('width', 420)
		if not objs:
			return []
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY :
			report_dict['no_resource'] = len(objs)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)

	def makeBackhaulMonthlyTable(self, report_dict):
		headings = [('sno', 'S.No', '5%', 'left'), ('interface_name', 'Interface Name', '20%', 'left'),]
		csv_headings = [('sno', 'S.No', 25), ('interface_name', 'Interface Name', 75),]
		# Selected Statistics
		stat_headings, stat_csv_headings = self.getHeadingsForStatistics(report_dict)
		headings += stat_headings
		csv_headings += stat_csv_headings
		headings += [	('util_in_out_events', 'No. of Bandwidth Choke Alerts', '10%', 'left'),
				('interface_down', 'No. of Backhaul Failures', '10%', 'left'),
				('outage', 'Total Downtime Duration', '10%', 'left'),
				('uptime', 'Total Uptime Duration', '10%', 'left'),
				('link_performance', 'Link Performance in %', '10%', 'left'),
				('mttr', 'MTTR', '10%', 'left'),
			]
		csv_headings += [  ('util_in_out_events', 'No. of Bandwidth Choke Alerts', 30),
				    ('interface_down', 'No. of Backhaul Failures', 25),
				    ('outage', 'Total Downtime Duration', 25),
				    ('uptime', 'Total Uptime Duration', 22),
				    ('link_performance', 'Link Performance', 20),
				    ('mttr', 'MTTR', 10),
				]
		objs = []
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		if not safe_int(report_dict.get('topn')):
			# checking the no of records to be queried with the resid, statid combination.
			# if the record exceeds the number configured MAX_RECORDS_FOR_SPLIT_QUERY in maxInputStats
			# then split query technique will be used to generate the large chunk of data
			countStats, split_query_list, tables, high_resolution_tables, event_tables = self.getBackhaulStatObjsLength(report_dict)
			if safe_int(countStats) > safe_int(maxInputStats.MAX_RECORDS_FOR_REPORTS_DISPLAY):
				return {"maxLimitReached": 1}
			# checking if the countStats is greater than the MAX_RECORDS_FOR_SPLIT_QUERY in maxInputStats
			if countStats > maxInputStats.MAX_RECORDS_FOR_SPLIT_QUERY and split_query_list:
				self.largeReportInProgress = 1
				#logMsg(2, "Call makeSplitQueryReport...", self.module_name)
				link, filePath = self.makeSplitQueryReport(split_query_list, report_dict, tables, high_resolution_tables, event_tables, csv_headings)
				self.largeReportInProgress = 0
				return {"isSplitQuery": 1, "report_link":link, "file_link":filePath}
		
		# Get the statistics values
		report_dict['stat_values'] = self.getBackhaulStatObjs(report_dict, aggregation=report_dict.get('aggregation', 'sum'))
		resids = report_dict.get('data_resids', [])
		# for backhaul alerts
		report_dict['backhaul_alerts'] = self.getBackhaulChokeAlerts(report_dict)
		# do downtime calculations
		report_dict['all_outage_by_resid'] = self.doBackhaulMonthlyCalculations(report_dict)
		objs = self.getBackhaulMonthlyAggregatedData(report_dict)
		width = report_dict.get('width', 420)
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY :
			report_dict['no_resource'] = len(objs)
		return XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width, csv_schema=csv_headings)

	def getVSATStatObjs(self, report_dict, aggregation='sum'):
		if aggregation == 'sum':
			formula = " SUM(s.avg*s.count) avg "
		else:
			formula = " AVG(s.avg) avg "
		return self.getStatObjs(report_dict, formula, aggregation=aggregation)

	def getTrapsSummary(self, report_dict):
		db_type = getDBType()
		events = []
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		top = safe_int(report_dict.get('topn'))
		event_tables = self.trap_db.getTables(start_time, end_time)
		res_table = report_dict.get('res_query', '')
		stats = report_dict.get('report_obj', {}).get('stats', [])
		trap_msgs = map(lambda a: a.strip(), string.split(stats, '|'))
		msg_filter = ''
		if '*' not in trap_msgs:
			trap_messages = trap_msgs[:]
		else:
			trap_messages = []
			if self.resource_manager.profiles.has_key('vsattrapmsg.cfg'):
				trap_messages = map(lambda a: a.get('name', ''), self.resource_manager.profiles['vsattrapmsg.cfg'].getOutputStat())
		if trap_messages:
			trap_messages = unique(map(lambda a: a.replace('!!', ';').replace('$$', ','), trap_messages))
			msg_filter = ' AND (%s) '%string.join(map(lambda a: " e.msg ILIKE '%s'" %(a), trap_messages), ' OR ')
		top_str = ''
		if top:
			if db_type == 'MySQL':
				top_str = ' LIMIT %s' %(top)
			elif db_type == 'MSSQL':
				top_str = ' TOP %s ' %(top)
			else:
				top_str = ' LIMIT %s' %(top)

		events_dict = {}

		# checking the count of enteries in the trap summary
		eventCount = 0
		event_table_count_map = {}
		for event_table in event_tables:
			if db_type:
				#<Robinson Panicker 20120515> : added one more filter [r.poller_id = n.poller_id] in query to avoid the IP Clashed VSATS name to be displayed instead of original one.
				sql_stmt = "SELECT count(r.resid) as evtcount FROM tblResConfig r, tblNodeInfo n, tblExtraNodeInfo en,(SELECT COUNT(*) occurance, e.resid FROM %s e, (%s) r WHERE e.timeStamp BETWEEN %s AND %s  AND e.resid = r.resid  %s  group by e.resid) s WHERE r.resid = s.resid AND r.poll_addr = n.poll_addr AND r.poller_id = n.poller_id AND n.nodeid = en.nodeid AND r.isDeleted = 0"%(event_table, res_table, start_time, end_time, msg_filter)
				logMsg(2, 'Find total objs sql_stmt - %s' %(sql_stmt), self.module_name)
				countObjs = DatabaseServer.executeSQLStatement(sql_stmt)
				if countObjs:
					event_table_count_map[event_table] = safe_int(countObjs[0].get('evtcount', 0))
					eventCount = eventCount + safe_int(countObjs[0].get('evtcount', 0))
		logMsg(2, "count -- %s "%(eventCount), self.module_name)
		if safe_int(eventCount) > safe_int(maxInputStats.MAX_RECORDS_FOR_REPORTS_DISPLAY):
			return {"maxLimitReached": 1}
		#return []
		# for collecting all the queries in case of events exceeding the MAX_RECORDS_FOR_SPLIT_QUERY
		split_query_list = []
		# checking ends here
		for event_table in event_tables:
			if db_type:
				#<Robinson Panicker 20120515> : added one more filter [r.poller_id = n.poller_id] in query to avoid the IP Clashed VSATS name to be displayed instead of original one.
				sql_stmt = "SELECT s.occurance, r.resid, r.poll_addr, r.params, n.hostname,n.sysLocation, en.region, en.conperson, en.contact_no, en.address FROM tblResConfig r, tblNodeInfo n, tblExtraNodeInfo en,(SELECT COUNT(*) occurance, e.resid FROM %s e, (%s) r WHERE e.timeStamp BETWEEN %s AND %s  AND e.resid = r.resid  %s  group by e.resid) s WHERE r.resid = s.resid AND r.poll_addr = n.poll_addr  AND r.poller_id = n.poller_id AND n.nodeid = en.nodeid AND r.isDeleted = 0 ORDER BY s.occurance DESC"%(event_table, res_table, start_time, end_time, msg_filter)
				logMsg(2, 'get occurance and objs sql_stmt - %s' %(sql_stmt), self.module_name)
				# checking if the eventCount is greater than the MAX_RECORDS_FOR_SPLIT_QUERY
				if 0:# dont know what to do here eventCount > maxInputStats.MAX_RECORDS_FOR_SPLIT_QUERY:
					#<Robinson Panicker 20120515> : added one more filter [r.poller_id = n.poller_id] in query to avoid the IP Clashed VSATS name to be displayed instead of original one.
					sql_stmt = "SELECT ROW_NUMBER() OVER(ORDER BY s.occurance DESC) rownum, s.occurance, r.resid, r.poll_addr, r.params, n.hostname,n.sysLocation, en.region, en.conperson, en.contact_no, en.address FROM tblResConfig r, tblNodeInfo n, tblExtraNodeInfo en,(SELECT COUNT(*) occurance, e.resid FROM %s e, (%s) r WHERE e.timeStamp BETWEEN %s AND %s  AND e.resid = r.resid  %s  group by e.resid) s WHERE r.resid = s.resid AND r.poll_addr = n.poll_addr  AND r.poller_id = n.poller_id AND n.nodeid = en.nodeid AND r.isDeleted = 0"%(event_table, res_table, start_time, end_time, msg_filter)
					logMsg(2, 'get objs batch by batch sql_stmt - %s' %(sql_stmt), self.module_name)
					split_query_list.append((sql_stmt, event_table_count_map.get(event_table, 0)))
				else:	
					objs = DatabaseServer.executeSQLStatement(sql_stmt)
					if objs != [] and objs != -1:
						for obj in objs:
							resid = safe_int(obj.get('resid'))
							if resid not in events_dict.keys():
								events_dict.update({resid:obj})
							else:
								events_dict[resid]['occurance'] = safe_int(events_dict.get(resid,{}).get('occurance')) + safe_int(obj.get('occurance'))
		
		# checking if split query has to be executed
		if split_query_list:
			self.largeReportInProgress = 1
			link, filePath = self.makeSplitQueryReport(split_query_list, report_dict)
			self.largeReportInProgress = 0
			return {"isSplitQuery": 1, "report_link":link, "file_link":filePath}
		# split query ends here
		events = events_dict.values()
		# Added by Deepak to get the top rows based on the occurance of events
		events_with_count = map(lambda a: (safe_int(a.get('occurance')), a), events)
		#logMsg(2, 'Total events -- %s' %(len(events)), self.module_name)
		events_with_count.sort(sortTuple1)
		events_with_count.reverse()
		events = map(lambda a: a[1], events_with_count)
		if top:
			events = events[:top]
		# Added by Deepak to get the top rows based on the occurance of events ends here
		#logMsg(2, 'Total events -- %s' %(len(events)), self.module_name)
		return events

	def doVSATCalculations(self, start_time, end_time, report_dict):
		"""
		Based on outage, to do the downtime calculation
		"""
		try:
			resolution = report_dict.get('resolution')
			high_resolution = report_dict.get('high_resolution')
			res_query = report_dict.get('res_query','')
			tables, high_resolution_tables = self.getTables(start_time, end_time, resolution, high_resolution)
			all_objs = []
			"""
				Since it is checking from the Time series table for the downtime of the statistics.
				So, data handling should be done here first.
			"""
			# Developer Roopesh : for handling the large report generation
			# calculating the no of data to be queried from the statistics
			sql_stmt_list = []
			count_data = 0
			for table in tables:
				sql_stmt = "SELECT s.resid, SUM(s.avg) avg FROM %s s, (%s) r WHERE r.resid = s.resid AND s.statid = %d AND timeStamp BETWEEN %d AND %d GROUP BY s.resid" %(table, res_query, safe_int(self.rawdn_id_map.get('downtime')), safe_int(start_time), safe_int(end_time))
				sql_stmt_list.append(sql_stmt)
			sql_stmt_all = string.join(sql_stmt_list, ' UNION ALL ')
			# query for taking the count from the combined queries
			if sql_stmt_all != "":
				sql_stmt_count = "SELECT count(*) as count  FROM(" + sql_stmt_all + ") as a"
				count_obj = DatabaseServer.executeSQLStatement(sql_stmt_count)
				count_data = count_obj[0].get("count", 0)
			# if the count is greater than maxInputStats.MAX_RECORDS_FOR_SPLIT_QUERY and maxInputStats.ENABLE_SPLIT_QUERY_FLAG is 1
			# Developer Roopesh : for handling the large report generation
			if count_data > maxInputStats.MAX_RECORDS_FOR_SPLIT_QUERY and maxInputStats.ENABLE_SPLIT_QUERY_FLAG:
				try:
					sql_stmt = "SELECT a.resid, SUM(a.avg) avg FROM (" + sql_stmt_all + ") as a GROUP BY a.resid"
					all_objs = DatabaseServer.executeSQLStatement(sql_stmt)
				except:
					logMsg(2,"Exception in Quering doVSATCalculations", self.module_name)
					all_objs = []
			else:
				for table in tables:
					sql_stmt = "SELECT s.resid, SUM(s.avg) avg FROM %s s, (%s) r WHERE r.resid = s.resid AND s.statid = %d AND timeStamp BETWEEN %d AND %d GROUP BY s.resid" %(table, res_query, safe_int(self.rawdn_id_map.get('downtime')), safe_int(start_time), safe_int(end_time))
					objs = DatabaseServer.executeSQLStatement(sql_stmt)
					if objs and objs != -1:
						all_objs += objs
			# Total  duration 
			if time.time() < end_time:
				total_time = time.time() - start_time + 1
			else:
				total_time = end_time - start_time + 1
			all_data_by_resid = {}
			# get the total downtime
			for data in all_objs:
				resid = safe_int(data.get('resid'))
				if not all_data_by_resid.has_key(resid):
					obj = {'outage': data.get('avg')}
				else:
					outage = all_data_by_resid.get(resid).get('outage') + data.get('avg')
					obj = {'outage': outage}
				all_data_by_resid.update({resid : obj})
			# get the total uptime
			for resid, obj in all_data_by_resid.items():
				uptime = total_time - safe_float(obj.get('outage'))
				if uptime < 0:
					uptime = 0
				obj['uptime'] = uptime
			return all_data_by_resid
		except Exception, msg:
			logExceptionMsg(4, 'Exception in doVSATCalculations - %s' %(msg), self.module_name)
			return {}

	def getVSATWiseSummaryData(self, report_dict):
		objs = []
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		unit_division_factor_map = {'bps': 1000, 'Bytes': 1024, '*bytes': 1024, 'bytes': 1024, '**bytes': 1024}
		vsat_info = self.getVSATsBasedOnGroup(report_dict, group_type='vsat')
		vsat_objs = vsat_info.items()
		vsat_objs.sort(sortTuple1)
		sno = 1
		stat_values = report_dict.get('stat_values', {})
		all_outage_by_resid = report_dict.get('all_outage_by_resid', {})
		for vsatid, resid in vsat_objs:
			obj = {'sno': sno, 'vsatid': vsatid}
			sno += 1
			# Aggregated value
			for stat in stats:
				stat_value = safe_float(stat_values.get(resid))
				unit = self.statid_unit_map.get(stat)
				if unit in self.units_to_be_formatted:
					division_factor = math.pow(unit_division_factor_map.get(unit, 1000), stat_unit)
					stat_value = stat_value / division_factor
				obj[stat] = '%.3f' %stat_value
			calc_obj = all_outage_by_resid.get(resid, {})
			uptime = safe_float(calc_obj.get('uptime'))
			outage = safe_float(calc_obj.get('outage'))
			obj['uptime'] = uptime
			obj['_uptime'] = formatHHMMSS(uptime)
			total_duration = uptime + outage
			obj['total_duration'] = total_duration
			obj['_total_duration'] = formatHHMMSS(total_duration)
			link_performance = 0
			if total_duration:
				link_performance = safe_float(uptime)/total_duration * 100
			obj['link_performance'] = '%.3f' %link_performance
			#Formula mentioned in excel sheet
			obj['uptime_vsat_in_days'] = '%.3f' %(safe_float(uptime)/86400)
			objs.append(obj)
		return objs

	def getPlatformWiseSummaryData(self, report_dict):
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		unit_division_factor_map = {'bps': 1000, 'Bytes': 1024, '*bytes': 1024, 'bytes': 1024, '**bytes': 1024}
		platform_resids = self.getVSATsBasedOnGroup(report_dict, group_type='platform')
		platforms = platform_resids.keys()
		platforms.sort()
		objs = []
		platform_objs = self.unms.vsat_diagnostics.platform.getObjs()
		platform_name_map = {}
		map(lambda a, b=platform_name_map: b.update({a.get('dn', ''): a.get('platformname')}), platform_objs)
		sno = 1
		all_outage_by_resid = report_dict.get('all_outage_by_resid', {})
		stat_values = report_dict.get('stat_values', {})
		for platform in platforms:
			platform_info = platform_resids.get(platform, {})
			no_of_vsats = len(platform_info.get('vsats', []))
			platform_name = platform_name_map.get(platform, platform)
			division_factor = math.pow(1000, stat_unit)
			if platform_info.get('bandwidth'):
				bandwidth = safe_int(platform_info.get('bandwidth') / division_factor)
			else:
				bandwidth = '-'
			obj = {'_platform': platform_name, 'purchased_bandwidth': bandwidth, 'no_of_vsats': no_of_vsats}
			sum_uptime = 0
			sum_outage = 0
			stat_obj = {}
			for resid in platform_info.get('vsats', []):
				calc_obj = all_outage_by_resid.get(resid, {})
				sum_uptime += safe_float(calc_obj.get('uptime'))
				sum_outage += safe_float(calc_obj.get('outage'))
				for stat in stats:
					stat_obj['temp_%d'%stat] = safe_float(stat_obj.get('temp_%d'%stat)) + safe_float(stat_values.get((resid, stat)))
			# Aggregated value
			for stat in stats:
				stat_value = safe_float(stat_obj.get('temp_%d'%stat))
				unit = self.statid_unit_map.get(stat)
				if unit in self.units_to_be_formatted:
					division_factor = math.pow(unit_division_factor_map.get(unit, 1000), stat_unit)
					stat_value = stat_value / division_factor
				obj[stat] = '%.3f' %( stat_value / no_of_vsats)
			obj['sno'] = sno
			sno += 1
			obj['uptime'] = sum_uptime
			obj['_uptime'] = formatHHMMSS(sum_uptime)
			total_duration = sum_uptime + sum_outage
			obj['total_duration'] = total_duration
			obj['_total_duration'] = formatHHMMSS(total_duration)
			link_performance = 0
			if total_duration:
				link_performance = safe_float(sum_uptime)/total_duration * 100
			obj['link_performance'] = '%.3f' %link_performance
			#Formula mentioned in excel sheet
			if no_of_vsats:
				uptime_vsat_in_days = safe_float(sum_uptime)/no_of_vsats/86400
				obj['uptime_vsat_in_days'] = '%.3f' %uptime_vsat_in_days
			objs.append(obj)
		return objs

	def getCustomerWiseSummaryData(self, report_dict):
		objs = []
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		unit_division_factor_map = {'bps': 1000, 'Bytes': 1024, '*bytes': 1024, 'bytes': 1024, '**bytes': 1024}
		stat_values = report_dict.get('stat_values', {})
		all_outage_by_resid = report_dict.get('all_outage_by_resid', {})
		customer_platform_resids = self.getVSATsBasedOnGroup(report_dict, group_type='customer')
		customer_platform_info = customer_platform_resids.items()
		customer_platform_info.sort(sortTuple1)
		sno = 1
		for customer, platform_resids in customer_platform_info:
			for platform, platform_info in platform_resids.items():
				obj = {'customer_name': customer}
				no_of_vsats = len(platform_info.get('vsats', []))
				division_factor = math.pow(1000, stat_unit)
				if platform_info.get('bandwidth'):
					bandwidth = safe_int(platform_info.get('bandwidth'))
				else:
					bandwidth = '-'
				obj['platform'] = platform
				obj['purchased_bandwidth'] = bandwidth
				obj['no_of_vsats'] = no_of_vsats
				sum_uptime = 0
				sum_outage = 0
				stat_obj = {}
				for resid in platform_info.get('vsats', []):
					calc_obj = all_outage_by_resid.get(resid, {})
					sum_uptime += safe_float(calc_obj.get('uptime'))
					sum_outage += safe_float(calc_obj.get('outage'))
					for stat in stats:
						stat_obj['temp_%d'%stat] = safe_float(stat_obj.get('temp_%d'%stat)) + safe_float(stat_values.get((resid, stat)))
				# Aggregated value
				for stat in stats:
					stat_value = safe_float(stat_obj.get('temp_%d'%stat))
					unit = self.statid_unit_map.get(stat)
					if unit in self.units_to_be_formatted:
						division_factor = math.pow(unit_division_factor_map.get(unit, 1000), stat_unit)
						stat_value = stat_value / division_factor
					obj[stat] = '%.3f' %(stat_value / no_of_vsats)
				uptime = sum_uptime
				obj['sno'] = sno
				sno += 1
				obj['uptime'] = uptime
				obj['_uptime'] = formatHHMMSS(uptime)
				total_duration = sum_uptime + sum_outage
				obj['total_duration'] = total_duration
				obj['_total_duration'] = formatHHMMSS(total_duration)
				link_performance = 0
				if total_duration:
					link_performance = safe_float(uptime)/total_duration * 100
				obj['link_performance'] = '%.3f' %link_performance
				#Formula mentioned in excel sheet
				if no_of_vsats:
					uptime_vsat_in_days = safe_float(uptime)/no_of_vsats/86400
					obj['uptime_vsat_in_days'] = '%.3f' %uptime_vsat_in_days
				objs.append(obj)
		return objs

	def getColumnWiseTotalSummaryData(self, objs, report_dict):
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		total_no_of_vsats = sum(map(lambda a : safe_int(a.get("no_of_vsats")), objs))
		total_purchased_bandwidth = sum(map(lambda a : safe_int(a.get("purchased_bandwidth")), objs))
		total_uptime = sum(map(lambda a : a.get("uptime"), objs))
		total_total_duration = sum(map(lambda a : a.get("total_duration"), objs))
		total_link_performance = 0
		if total_total_duration:
			total_link_performance = safe_float(total_uptime)/total_total_duration * 100
		total_uptime_vsat_in_days = 0
		if total_no_of_vsats:
			total_uptime_vsat_in_days = safe_float(total_uptime)/total_no_of_vsats/86400
		total_obj = {	
				'platform': ' ', 
				'customer_name': 'Total',
				'vsatid': 'Total',
				'_platform': 'VSAT Hub Network',
				'no_of_vsats': total_no_of_vsats, 
				'purchased_bandwidth': str(total_purchased_bandwidth),
				'_uptime': formatHHMMSS(total_uptime), 
				'_total_duration': formatHHMMSS(total_total_duration),
				'link_performance': '%.3f' %(total_link_performance),
				'uptime_vsat_in_days': '%.3f' %(total_uptime_vsat_in_days),
				'sno': ' ',
			}
		for stat in stats:
			total_obj[stat] = '%.3f' %(sum(map(lambda a: safe_float(a.get(stat)), objs)))
		return total_obj

	def getVSATWiseAggregateObjs(self, report_dict):
		st_time = safe_int(time.time())
		vsat_info = self.getVSATsBasedOnGroup(report_dict, group_type='vsat')
		et_time = safe_int(time.time())
		logMsg(2, 'Total time taken to get the VSATs based on VSAT Group is %d' %((et_time-st_time)), self.module_name)
		vsat_objs = vsat_info.items()
		vsat_objs.sort(sortTuple1)
		sno = 1
		t1_all_outage_by_resid = report_dict.get('t1_all_outage_by_resid', {})
		t2_all_outage_by_resid = report_dict.get('t2_all_outage_by_resid', {})
		objs = []
		for vsatid, resid in vsat_objs:
			obj = {'sno': sno, 'vsatid': vsatid}
			sno += 1
			calc_obj = t1_all_outage_by_resid.get(resid, {})
			uptime1 = safe_float(calc_obj.get('uptime'))
			outage1 = safe_float(calc_obj.get('outage'))
			calc_obj = t2_all_outage_by_resid.get(resid, {})
			uptime2 = safe_float(calc_obj.get('uptime'))
			outage2 = safe_float(calc_obj.get('outage'))
			total_duration1 = uptime1 + outage1
			link_performance1 = 0
			if total_duration1:
				link_performance1 = safe_float(uptime1)/total_duration1 * 100
			obj['nw_uptime1'] = link_performance1
			total_duration2 = uptime2 + outage2
			link_performance2 = 0
			if total_duration2:
				link_performance2 = safe_float(uptime2)/total_duration2 * 100
			obj['nw_uptime2'] = link_performance2
			obj['performance'] = link_performance2 - link_performance1
			objs.append(obj)
		return objs

	def getCustomerWiseAggregateObjs(self, report_dict):
		st_time = safe_int(time.time())
		customer_platform_resids = self.getVSATsBasedOnGroup(report_dict, group_type='customer')
		et_time = safe_int(time.time())
		logMsg(2, 'Total time taken to get the VSATs based on Customer Group is %d' %((et_time-st_time)), self.module_name)
		customer_platform_info = customer_platform_resids.items()
		customer_platform_info.sort(sortTuple1)
		sno = 1
		t1_all_outage_by_resid = report_dict.get('t1_all_outage_by_resid', {})
		t2_all_outage_by_resid = report_dict.get('t2_all_outage_by_resid', {})
		objs = []
		for customer, platform_resids in customer_platform_info:
			for platform, platform_info in platform_resids.items():
				obj = {'customer_name': customer}
				obj['platform'] = platform
				sum_uptime1 = 0
				sum_outage1 = 0
				sum_uptime2 = 0
				sum_outage2 = 0
				for resid in platform_info.get('vsats', []):
					calc_obj = t1_all_outage_by_resid.get(resid, {})
					sum_uptime1 += safe_float(calc_obj.get('uptime'))
					sum_outage1 += safe_float(calc_obj.get('outage'))
					calc_obj = t2_all_outage_by_resid.get(resid, {})
					sum_uptime2 += safe_float(calc_obj.get('uptime'))
					sum_outage2 += safe_float(calc_obj.get('outage'))
				obj['sno'] = sno
				sno += 1
				total_duration1 = sum_uptime1 + sum_outage1
				link_performance1 = 0
				if total_duration1:
					link_performance1 = safe_float(sum_uptime1)/total_duration1 * 100
				obj['nw_uptime1'] = '%.3f' %link_performance1
				total_duration2 = sum_uptime2 + sum_outage2
				link_performance2 = 0
				if total_duration2:
					link_performance2 = safe_float(sum_uptime2)/total_duration2 * 100
				obj['nw_uptime2'] = '%.3f' %link_performance2
				obj['performance'] = link_performance2 - link_performance1
				objs.append(obj)
		return objs

	def getPlatformWiseAggregateObjs(self, report_dict):
		st_time = safe_int(time.time())
		platform_resids = self.getVSATsBasedOnGroup(report_dict, group_type='platform')
		et_time = safe_int(time.time())
		logMsg(2, 'Total time taken to get the VSATs based on Platform Group is %d' %((et_time-st_time)), self.module_name)
		platforms = platform_resids.keys()
		platforms.sort()
		objs = []
		platform_objs = self.unms.vsat_diagnostics.platform.getObjs()
		platform_name_map = {}
		map(lambda a, b=platform_name_map: b.update({a.get('dn', ''): a.get('platformname')}), platform_objs)
		sno = 1
		t1_all_outage_by_resid = report_dict.get('t1_all_outage_by_resid', {})
		t2_all_outage_by_resid = report_dict.get('t2_all_outage_by_resid', {})
		for platform in platforms:
			platform_info = platform_resids.get(platform, {})
			platform_name = platform_name_map.get(platform, platform)
			obj = {'_platform': platform_name}
			sum_uptime1 = 0
			sum_outage1 = 0
			sum_uptime2 = 0
			sum_outage2 = 0
			for resid in platform_info.get('vsats', []):
				calc_obj = t1_all_outage_by_resid.get(resid, {})
				sum_uptime1 += safe_float(calc_obj.get('uptime'))
				sum_outage1 += safe_float(calc_obj.get('outage'))
				calc_obj = t2_all_outage_by_resid.get(resid, {})
				sum_uptime2 += safe_float(calc_obj.get('uptime'))
				sum_outage2 += safe_float(calc_obj.get('outage'))
			obj['sno'] = sno
			sno += 1
			total_duration1 = sum_uptime1 + sum_outage1
			link_performance1 = 0
			if total_duration1:
				link_performance1 = safe_float(sum_uptime1)/total_duration1 * 100
			obj['nw_uptime1'] = '%.3f' %link_performance1
			total_duration2 = sum_uptime2 + sum_outage2
			link_performance2 = 0
			if total_duration2:
				link_performance2 = safe_float(sum_uptime2)/total_duration2 * 100
			obj['nw_uptime2'] = '%.3f' %link_performance2
			obj['performance'] = link_performance2 - link_performance1
			objs.append(obj)
		return objs

	def getTrapsCount(self, report_dict):
		db_type = getDBType()
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		logMsg(2, (start_time, end_time))
		top = safe_int(report_dict.get('topn'))
		event_tables = self.trap_db.getTables(start_time, end_time)
		stats = report_dict.get('report_obj', {}).get('stats', [])
		trap_msgs = map(lambda a: a.strip(), string.split(stats, '|'))
		time_filter = timeBetween(start_time, end_time, report_dict.get('timebetween',''))
		msg_filter = ''
		if '*' not in trap_msgs:
			trap_messages = trap_msgs[:]
		else:
			trap_messages = []
			if self.resource_manager.profiles.has_key('vsattrapmsg.cfg'):
				trap_messages = map(lambda a: a.get('name', ''), self.resource_manager.profiles['vsattrapmsg.cfg'].getOutputStat())
		if trap_messages:
			trap_messages = unique(map(lambda a: a.replace('!!', ';').replace('$$', ','), trap_messages))
			msg_filter = ' AND (%s) '%string.join(map(lambda a: " e.msg ILIKE '%%%s%%'" %(a), trap_messages), ' OR ')
		top_str = ''
		if top:
			top_str = ' LIMIT %s' %(top)
		
		events = []
		logMsg(2, 'top - %s and top_str - %s' %(top, top_str), self.module_name)
		res_table = report_dict.get('res_query', '')
		resid_count_map = {}
		sql_stmt_list = []
		# commenting ends here.
		# implementing the previous implementation done as it will give the occurance of the selected events in a given day.
		# the event if occur in that day taken from the respective table trap will make a count of 1.
		# so the count of 1 will be made for each day if it occurs at all.
		for event_table in event_tables:
			sql_stmt = "SELECT COUNT(*) event_count, e.resid FROM %s e, (%s) r WHERE e.timeStamp BETWEEN %s AND %s AND e.resid = r.resid %s GROUP BY e.resid"%(event_table, res_table, start_time, end_time, msg_filter)
			logMsg(2, 'sql_stmt - %s' %(sql_stmt), self.module_name)
			objs = DatabaseServer.executeSQLStatement(sql_stmt)
			if objs != [] and objs != -1:
				for obj in objs:
					resid = safe_int(obj.get('resid'))
					resid_count_map[resid] = safe_int(resid_count_map.get(resid)) + 1		
			del objs
		logMsg(2, '##resid_count_map - %s' %(resid_count_map), self.module_name)
		return resid_count_map

	def getBackhaulStatObjsLength(self, report_dict):
		try:
			start_time = report_dict.get('st')
			end_time = report_dict.get('et')
			stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
			res_query = report_dict.get('res_query')
			resolution = report_dict.get('resolution')
			high_resolution = report_dict.get('high_resolution')
			res_query = report_dict.get('res_query','')
			tables, high_resolution_tables = self.getTables(start_time, end_time, resolution, high_resolution)
			all_objs = []
			aggregation=report_dict.get('aggregation', 'sum')
			if aggregation == 'sum':
				formula = " SUM(s.avg) avg "
			else:
				formula = " AVG(s.avg) avg "
			countObjs = 0
			split_query_list = []
			for table in tables:
				sql_stmt = "SELECT count(*) AS count from (SELECT s.resid, s.statid, %s FROM %s s, (%s) r WHERE r.resid = s.resid AND s.statid IN (%s) AND timeStamp BETWEEN %d AND %d GROUP BY s.resid, s.statid) as t" %(formula, table, res_query, str(stats)[1:-1], start_time, end_time)
				#logMsg(2, 'getQueryCountFromTables sql_stmt -- %s' %sql_stmt, self.module_name)
				objs = DatabaseServer.executeSQLStatement(sql_stmt)
				#logMsg(2, 'getQueryCountFromTables objs length -- %s' %(objs), self.module_name)
				sql_stmt_collect = "SELECT ROW_NUMBER() OVER(ORDER BY s.resid) rownum, s.resid, s.statid, %s FROM %s s, (%s) r WHERE r.resid = s.resid AND s.statid IN (%s) AND timeStamp BETWEEN %d AND %d GROUP BY s.resid, s.statid" %(formula, table, res_query, str(stats)[1:-1], start_time, end_time)
				split_query_list.append((sql_stmt_collect, safe_int(objs[0].get("count", 0))))
				if objs and objs != -1:
					countObjs += safe_int(objs[0].get("count", 0))
			#logMsg(2, 'getQueryCountFromTables split_query_list -- %s' %(split_query_list), self.module_name)
			event_tables = self.event_db.getTables(start_time, end_time)
			return countObjs, split_query_list, tables, high_resolution_tables, event_tables
		except Exception, msg:
			logExceptionMsg(4, 'Exception  - in getQueryCount %s' %(msg), self.module_name)
	def getBackhaulStatObjs(self, report_dict, aggregation='sum'):
		if aggregation == 'sum':
			formula = " SUM(s.avg) avg "
		else:
			formula = " AVG(s.avg) avg "
		return self.getStatObjs(report_dict, formula, aggregation=aggregation)

	def doBackhaulDownTimeCalculations(self, report_dict):
		top_value = safe_int(report_dict.get('topn'))
		downtime_calc_objs = []
		all_outage_by_resid = {}
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		vsat_affected_calc = safe_int(report_dict.get('vsat_affected_calc'))
		resids = report_dict.get('data_resids', [])
		for resid in resids:
			# do the downtime calculation
			events = self.getEventsForInterface(resid, start_time, end_time)
			calc_objs, total_downtime = self.doDownTimeCalulation(events, resid, start_time, end_time, vsat_affected_calc)
			downtime_calc_objs.append((total_downtime, (resid, calc_objs)))
		downtime_calc_objs.sort(sortTuple1)
		downtime_calc_objs.reverse()
		# get the top value based on total downtime
		if top_value:
			downtime_calc_objs = downtime_calc_objs[:top_value]
		#logMsg(2, "downtime_calc_objs -- %s"%downtime_calc_objs, self.module_name)
		#logMsg(2, "downtime_calc_objs length -- %s"%(len(downtime_calc_objs)), self.module_name)
		return map(lambda a: a[1], downtime_calc_objs)

	def getBackhaulAggregatedData(self, report_dict):
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		unit_map = {-1: '', 0:'', 1:'K', 2:'M', 3:'G', 4:'T'}
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		vsat_affected_calc = safe_int(report_dict.get('vsat_affected_calc'))
		res_path_map = report_dict.get('res_path_map', {})
		stat_values = report_dict.get('stat_values', {})
		objs = []
		all_outage_by_resid = report_dict.get('all_outage_by_resid', [])
		sno = 1
		for resid, calc_objs in all_outage_by_resid:
			path_name = res_path_map.get(resid, '')
			#calc_objs = all_outage_by_resid.get(resid, [])		
			# Excel Report
			if len(calc_objs) > 1 and report_dict.get('fromPDF') and safe_int(report_dict.get('format_csv')) == 1:
				obj = {}
				for stat in stats:
					if stat == safe_int(self.statname_id_map.get('No of VSATs Up')):
						continue
					cur_unit =  self.statid_unit_map.get(stat, '')
					obj[stat] = formatToReqdUnit(stat_values.get((resid, stat)) ,cur_unit, stat_unit)
				obj['interface_name'] = path_name
				# Merge the cells
				obj['merge_cells'] = 1
				obj['sno'] = sno
				sno += 1
				link_downs = []
				link_ups = []
				outages = []
				vsats_affecteds = []
				for calc_obj in calc_objs:
					if calc_obj.get('link_down','') :
						lt = time.localtime(calc_obj.get('link_down'))
						if (end_time - start_time) > 86399:
							link_down = DateTime.DateTime(lt[0], lt[1], lt[2], lt[3], lt[4], lt[5])
						else:
							link_down = time.strftime('%H:%M:%S', lt)
					if safe_int(calc_obj.get('link_up','')) :
						lt = time.localtime(calc_obj.get('link_up'))
						if (end_time - start_time) > 86399:
							link_up = DateTime.DateTime(lt[0], lt[1], lt[2], lt[3], lt[4], lt[5])
						else:
							link_up = time.strftime('%H:%M:%S', lt)
					outage = formatValue(calc_obj.get('outage'), 'outagesec')
					vsats_affected = calc_obj.get('vsats_affected','-')
					if not vsats_affected:
						vsats_affected = '-'
					if link_down == '':
						link_down = '-'
					if link_up == '':
						link_up = '-'
					link_downs.append(link_down)
					link_ups.append(link_up)
					outages.append(outage)
					vsats_affecteds.append(vsats_affected)
				obj['link_down'] = link_downs
				obj['link_down_hhmmss'] = link_downs
				obj['link_up'] = link_ups
				obj['link_up_hhmmss'] = link_ups
				obj['outage'] = outages
				obj['vsats_affected'] = vsats_affecteds
				objs.append(obj)
			else:
				counter = 0
				for calc_obj in calc_objs:
					obj = {}
					if not counter:
						for stat in stats:
							if stat == safe_int(self.statname_id_map.get('No of VSATs Up')):
								continue
							cur_unit =  self.statid_unit_map.get(stat, '')
							if stat_values.get((resid, stat)):
								obj[stat] = formatToReqdUnit(stat_values.get((resid, stat)) ,cur_unit, stat_unit)
							"""
							if cur_unit in self.units_to_be_formatted:
								obj[stat] = formatToReqdUnit(stat_values.get((resid, stat)) ,cur_unit, stat_unit)
							else:
								obj[stat] = '%.3f' %stat_values.get((resid, stat))
							"""
						obj['interface_name'] = path_name
						obj['sno'] = sno
						sno += 1
					else:
						obj[id] = ' '
						obj['interface_name'] = ' '
						obj['sno'] = ' '
					if calc_obj.get('link_down','') :
						lt = time.localtime(calc_obj.get('link_down'))
						if (end_time - start_time) > 86399:
							# For Excel Report
							if report_dict.get('fromPDF') and safe_int(report_dict.get('format_csv')) == 1:
								obj['link_down'] = DateTime.DateTime(lt[0], lt[1], lt[2], lt[3], lt[4], lt[5])
							else:
								obj['link_down'] = time.strftime('%m/%d/%Y %H:%M:%S', lt)
						else:
							obj['link_down'] = time.strftime('%H:%M:%S', lt)
					if safe_int(calc_obj.get('link_up','')) :
						lt = time.localtime(calc_obj.get('link_up'))
						if (end_time - start_time) > 86399:
							# For Excel Report
							if report_dict.get('fromPDF') and safe_int(report_dict.get('format_csv')) == 1:
								obj['link_up'] = DateTime.DateTime(lt[0], lt[1], lt[2], lt[3], lt[4], lt[5])
							else:
								obj['link_up'] = time.strftime('%m/%d/%Y %H:%M:%S', lt)
						else:
							obj['link_up'] = time.strftime('%H:%M:%S', time.localtime(calc_obj.get('link_up')))
					obj['outage'] = formatValue(calc_obj.get('outage'), 'outagesec')
					obj['vsats_affected'] = calc_obj.get('vsats_affected','-')
					if not obj['vsats_affected']:
						obj['vsats_affected'] = '-'
					if obj.get('link_down','') == '':
						obj['link_down'] = '-'
					if obj.get('link_up','') == '':
						obj['link_up'] = '-'
					obj['link_up_hhmmss'] = obj.get('link_up','')
					obj['link_down_hhmmss'] = obj.get('link_down','')
					counter += 1
					objs.append(obj)
		return objs

	def doBackhaulMonthlyCalculations(self, report_dict):
		top_value = safe_int(report_dict.get('topn'))
		downtime_calc_objs = []
		all_outage_by_resid = {}
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		event_tables = self.event_db.getTables(start_time, end_time)
		backhaul_alerts = report_dict.get('backhaul_alerts', {})
		resids = report_dict.get('data_resids', [])
		for resid in resids:
			# do the downtime calculation
			events = self.getEventsForInterface(resid, start_time, end_time)
			outage_objs, downtime_total = self.doDownTimeCalulation(events, resid, start_time, end_time)
			#4: Total Uptime duration in minutes
			if time.time() < end_time:
				total_time = time.time() - start_time + 1
			else:
				total_time = end_time - start_time + 1
			uptime_total = total_time - downtime_total
			#5: link performance in % = (Total uptime in minutes)/ (Total down time in minutes + Total uptime in minutes) *100
			if downtime_total or uptime_total:
				link_performance = '%.3f' %(safe_float(uptime_total)/(downtime_total + uptime_total) * 100)
			interface_down_events_total = safe_int(backhaul_alerts.get(resid, {}).get('interface_down_events'))
			util_in_out_total = safe_int(backhaul_alerts.get(resid, {}).get('util_in_out_total'))
			#6: MTTR Mean time to repair : Total Downtime Duration in minutes /No. of Backhaul Failures
			mttr = ''
			if interface_down_events_total:
				mttr = downtime_total/interface_down_events_total
			calculatedObj = {
					'util_in_out_events':util_in_out_total,
					'interface_down': interface_down_events_total,
					'outage':downtime_total,
					'uptime':uptime_total,
					'link_performance':link_performance,
					'mttr':mttr
				}
			downtime_calc_objs.append((downtime_total, (resid, calculatedObj)))
		downtime_calc_objs.sort(sortTuple1)
		downtime_calc_objs.reverse()
		# get the top value based on total downtime
		if top_value:
			downtime_calc_objs = downtime_calc_objs[:top_value]
		return map(lambda a: a[1], downtime_calc_objs)

	def getBackhaulMonthlyAggregatedData(self, report_dict):
		stats = map(lambda a: safe_int(str(a).strip()), report_dict.get('report_obj', {}).get('statids', []))
		stat_unit = safe_int(report_dict.get('report_obj', {}).get('stat_unit',1))
		unit_map = {-1: '', 0:'', 1:'K', 2:'M', 3:'G', 4:'T'}
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		res_path_map = report_dict.get('res_path_map', {})
		stat_values = report_dict.get('stat_values', {})
		backhaul_alerts = report_dict.get('backhaul_alerts', {})
		objs = []
		all_outage_by_resid = report_dict.get('all_outage_by_resid', [])
		sno = 1
		for resid, calc_obj in all_outage_by_resid:
			path_name = res_path_map.get(resid, '')
			obj = {'interface_name': path_name}
			obj.update(calc_obj)
			for stat in stats:
				cur_unit =  self.statid_unit_map.get(stat, '')
				if stat_values.get((resid, stat)):
					obj[stat] = formatToReqdUnit(stat_values.get((resid, stat)) ,cur_unit, stat_unit)
			objs.append(obj)
		objs.sort(lambda a, b, c = [(1,'link_performance')] : cmpField(a, b, c))
		#To calculate the column wise total
		total_bandwidth_choks = sum(map(lambda a : safe_int(a.get("util_in_out_events")), objs))
		total_backhaul_failures = sum(map(lambda a : safe_int(a.get("interface_down")), objs))
		total_downtime = sum(map(lambda a : a.get("outage"), objs))
		total_uptime = sum(map(lambda a : a.get("uptime"), objs))
		avg_link_performance = 0
		avg_mttr = 0
		if objs:
			logMsg(2, 'link_performance -> %s'%(objs), self.module_name)
			avg_link_performance = sum(map(lambda a : safe_float(a.get("link_performance")), objs))/len(objs)
			avg_mttr = sum(map(lambda a : safe_float(a.get("mttr")), objs))/len(objs)
		total_obj = {	
				'interface_name': 'Total', 
				'util_in_out_events': total_bandwidth_choks,
				'interface_down': total_backhaul_failures, 
				'outage': formatHHMMSS(total_downtime),
				'uptime':formatHHMMSS(total_uptime),
				'link_performance': '%.3f' %(avg_link_performance),
				'mttr': formatHHMMSS(avg_mttr),
				'sno': ' ',
			}
		for stat in stats:
			cur_unit =  self.statid_unit_map.get(stat, '')
			total_obj[stat] = " "
		sno = 1
		for obj in objs:	
			obj['sno'] = sno
			obj['outage'] = formatHHMMSS(obj.get('outage'))
			obj['uptime'] = formatHHMMSS(obj.get('uptime'))
			obj['link_performance'] = '%.3f' %(safe_float(obj.get('link_performance')))
			obj['mttr'] = formatHHMMSS(obj.get('mttr'))
			'''
			if obj.get('mttr') :
				obj['mttr'] = formatHHMMSS(obj.get('mttr'))
			else :
				obj['mttr'] = 0 #formatHHMMSS(obj.get('mttr'))
				obj['mttr'] = formatHHMMSS(obj.get('mttr'))
			'''
			sno += 1
		objs.append(total_obj)
		return objs

	def getBackhaulChokeAlerts(self, report_dict):
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		event_tables = self.event_db.getTables(start_time, end_time)
		res_query = report_dict.get('res_query', '')
		backhaul_alerts = {}
		for event_table in event_tables:
			#1: no of bandwidth choke alerts i.e. No.of Util In & Out High  Events
			sql_stmt = "select r.resid, t.statid, COUNT(*) event_count from %s e, tblThresholds t, (%s) r WHERE r.resid = t.resid AND t.statid IN (%d, %d, %d) and e.fid = t.thresid and e.up_down=0 and e.timeStamp between %s and %s GROUP BY r.resid, t.statid"%(event_table, res_query, safe_int(self.rawdn_id_map.get('utilIn')), safe_int(self.rawdn_id_map.get('utilOut')), safe_int(self.rawdn_id_map.get('n_avail')), start_time, end_time)
			event_objs = DatabaseServer.executeSQLStatement(sql_stmt)
			if event_objs and event_objs !=-1:
				for event_obj in event_objs:
					resid = safe_int(event_obj.get('resid'))
					statid = safe_int(event_obj.get('statid'))
					if not backhaul_alerts.has_key(resid):
						backhaul_alerts[resid] = {'util_in_out_total': 0, 'interface_down_events': 0}
					if statid == safe_int(self.rawdn_id_map.get('n_avail')):
						backhaul_alerts[resid]['interface_down_events'] += safe_int(event_obj.get('event_count'))
					else:
						backhaul_alerts[resid]['util_in_out_total'] += safe_int(event_obj.get('event_count'))
		return backhaul_alerts

	def makeSplitQueryReport(self, split_query_list, report_dict, tables = [], high_resolution_tables = [], event_tables = [], csv_headings=[]):
		try:
			logMsg(2, "Entering the makeSplitQueryReport...", self.module_name)
			# query_to_split is the list of tuple in [(sqlQuery, query count)....] format
			no_of_records_to_query = maxInputStats.MAX_RECORDS_ONE_SHOT
			split_query_list_all = []
			titleText = report_dict['report_obj'].get('name', '')
			if report_dict.get('time_period_heading', '') != '':
				titleText = str(titleText) +  " for the Period " + str(report_dict.get('time_period_heading', ''))
			titleTab = report_dict['report_obj'].get('name', '')
			reportType = report_dict.get('report_type')
			# collecting all the queries
			for query in split_query_list:
				if query[1] > 0:
					sql_stmt = query[0]
					query_count = query[1]
					splited_query = self.getClusteredQueries(sql_stmt, query_count, no_of_records_to_query)
					split_query_list_all.extend(splited_query)
			logMsg(2, "split_query_list_all -- %s"%split_query_list_all, self.module_name)
			# for quering the data and generating the report one by one by calling the generateSplitQuerySheet function defined in pdfcreator.py
			link, filePath = self.unms.pdf_creator.generateSplitQueryReport(split_query_list_all, report_dict, reportType, titleText, titleTab, tables, high_resolution_tables, event_tables, csv_headings)
			return link, filePath
		except Exception, msg:
			logExceptionMsg(4, 'Exception in makeSplitQueryReport - %s' %(msg), self.module_name)
			return XMLErrorMessage(msg)

	def makeDataTableForVSATPing(self, params, request, isFormatChange=0):
		"""Draw the data table
		"""
		logMsg(2,'params - %s'%(params),'makeDataTable')
		time_stamps = params.get('data_time_stamps')
		datas = params.get('data')
		unit_map = params.get('legend_unit_map')
		gettimeStamps = params.get('data_time_stamps')
		time_format = params.get('data_time_format')
		excel_time_format = {
						'%H:%M:%S': '_time',
						'%H:%M': '_time',
						'%H:%M:00': '_time',
						'%H:00': '_time',
						'%m/%d/%Y %H:%M:%S': '_timeStamp', 
						'%m/%d/%Y %H:%M:00': '_timeStamp', 
						'%m/%d/%Y %H:%M': '_timeStamp', 
						'%m/%d/%Y %H:00': '_timeStamp',
						'%m/%d/%Y': '_date',
					}
		time_key = excel_time_format.get(time_format, '_dtime')
		red_val = safe_float(request.get('report_obj', {}).get('red_val', ''))
		yellow_val = safe_float(request.get('report_obj', {}).get('yellow_val', ''))
		logMsg(2,'time_key - %s,red_val - %s,yellow_val - %s'%(time_key,red_val,yellow_val),'makeDataTable')
		polarity = -1
		if red_val and yellow_val:
			polarity = iif(red_val >= yellow_val, 1, -1)
		threshold = [yellow_val, red_val, polarity]
		if request.get('fromPDF'):
			chart_time_format = params.get('time_format', '')
		else:
			if params.get('time_format', '') in [None, "None", "", -1, "Null"]:
				params['time_format'] = 'mm/dd/yy hh:mm'
			chart_time_format = string.replace(params.get('time_format', ''), ' ', '&nbsp;')
		excel_chart_time_format = string.strip(string.replace(params.get('time_format', ''), 'nn', 'mm'))
		chart_time_format = string.strip(string.replace(chart_time_format, 'nn', 'mm'))
		if time_format.endswith(':%M'):
			chart_time_format += ':ss'
			excel_chart_time_format += ':ss'
			time_format += ':%S'
		paths = unit_map.keys()
		paths = gettimeStamps
		col_width = 96 / (len(paths))
		time_col_width = '4%'
		if not col_width:
			time_col_width = str(col_width) + '%'
		#headings = [('time', 'Time %s' %(chart_time_format), time_col_width, 'left')]
		#csv_headings = [(time_key, 'Time %s' %(excel_chart_time_format), 25)]
		headings = [('_timeStamp', 'VSAT ID', time_col_width, 'left')]
		csv_headings = [('_timeStamp', 'VSAT ID', 25)]
		# Getting headings dynamically, based on the paths available
		index = 0
		logMsg(2,'paths - %s'%(paths),'makeDataTable')
		# Prepare time based datas
		if not request.get('fromPDF'):
			time_format = string.replace(time_format, ' ', '&nbsp;')
		paths.sort()
		for path in paths:
			# if params.get('chart_title'):
			# 	path = path + '(%s)' %params.get('chart_title', '')
			#headings.append(('path_%s' %(index), makeChoppedPath(path, 20), str(col_width) + '%', 'center'))
			lt = time.localtime(path)
			logMsg(2,'lt - %s'%(lt),'makeDataTable')
			#path =  DateTime.DateTime(lt[0], lt[1], lt[2], lt[3], lt[4], lt[5])
			
			path = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(path))
			logMsg(2,'path - %s'%(path),'makeDataTable')
			headings.append(('path_%s' %(index), path, str(col_width) + '%', 'center'))
			csv_headings.append(('_path_%s' %(index), path, 25))
			index += 1

		# For CSV Report
		if safe_int(request.get('format_csv')) == 2:
			csv_headings = headings[:]
		time_dict = {}
		legend_summary_map = {}
		timeStampToFormattedTime = {}
		formattedTimeToTimeStamp = {}
		for legend, data in datas:
			data_value = map(lambda a: a[1], data)
			# Customized Data Table for Displaying Average/Min/Max of Columns
			if maxInputStats.REPORT_COLUMN_SUMMARY_DATA:
				if not legend_summary_map.has_key(legend):
					legend_summary_map[legend] = {'avg': '-', 'min': '-', 'max': '-'}
				legend_summary_map[legend]['avg'] = '%.3f' %(avg(map(lambda a: safe_float(a), filter(lambda a: a != NULL_VALUE, data_value))))
				legend_summary_map[legend]['max'] = '%.3f' %(max(map(lambda a: safe_float(a), filter(lambda a: a != NULL_VALUE, data_value))))
				legend_summary_map[legend]['min'] = '%.3f' %(min(map(lambda a: safe_float(a), filter(lambda a: a != NULL_VALUE, data_value))))
			for time_stamp, value in data:
				if value == NULL_VALUE:
					continue
				formatted_time = time.strftime(time_format, time.localtime(time_stamp))
				if not time_dict.has_key(formatted_time):
					time_dict[formatted_time] = {}
				if not timeStampToFormattedTime.has_key(time_stamp) :
					timeStampToFormattedTime[time_stamp] = formatted_time
				time_dict[formatted_time][legend] = value
		for time_stamp, formatted_time in timeStampToFormattedTime.items():
			formattedTimeToTimeStamp[formatted_time] = time_stamp
		objs = []
		# sorting the datas based on the time.
		# TODO: need to do better solution
		report_time_stamps = timeStampToFormattedTime.items()
		report_time_stamps.sort(sortTuple1)
		temp_times = map(lambda a: a[1], report_time_stamps)
		formatted_report_times = []
		formatted_report_time_map = {}
		for temp_time in temp_times:
			if not formatted_report_time_map.has_key(temp_time):
				formatted_report_times.append(temp_time)
				formatted_report_time_map[temp_time] = 1
		temp_datas = []
		for formatted_time in formatted_report_times:
			temp_datas.append((formatted_time, time_dict[formatted_time]))
		# Construct the objs
		path_index_map = {}
		index = 0
		for path in paths:
			path_index_map[path] = index
			index += 1
		gif_prefix = request.get("severity_gif_name", request['request']["__user"].get("default_gif", "smiley"))
		division_factor = request.get('division_factor')
		actual_legend_unit_map = {}
		map(lambda a, b=actual_legend_unit_map: b.update(a), request.get('legend_unit_map', {}).values())
		objs =[]
		for path, data in datas:
			temp_data = data
			obj = {'_timeStamp': path + '(%s)'%(params.get('chart_title',''))}
			for timeStampCol, pollValue in temp_data:
				logMsg(2,'pollValue - %s'%(pollValue),'makeDataTable')
				newdata = formatValue(pollValue, unit_map.get(path))
				logMsg(2,'newdata - %s'%(newdata),'makeDataTable')
				obj['path_%s' %(path_index_map.get(timeStampCol))] = newdata
				obj['_path_%s' %(path_index_map.get(timeStampCol))] = newdata
			objs.append(obj)
				
		"""
		for time_stamp, datas in temp_datas:
			#obj = {'time': time_stamp}
			obj = {'time': 'Ku1_13000'}
			
			# Added For Excel Time Formatting.
			lt = time.localtime(formattedTimeToTimeStamp.get(time_stamp))
			obj[time_key] = DateTime.DateTime(lt[0], lt[1], lt[2], lt[3], lt[4], lt[5])
			# For Excel Time Formatting ends here
			for path, data in datas.items():
				temp_data = data
				logMsg(2,'temp_data - %s'%(temp_data),'makeDataTable')
				if isFormatChange:
					data = formatValue(safe_int(data),isFormatChange)
				elif data: 
					data = formatValue(data, unit_map.get(path))
				if not request.get('fromPDF') and request.get('topn'):
					actual_unit = actual_legend_unit_map.get(path)
					if actual_unit in self.units_to_be_formatted and actual_unit != 'sec':
						temp_division_factor = division_factor
					else:
						temp_division_factor = 1
					color_index = self.getThresholdColor(threshold, safe_float(temp_data) * temp_division_factor)
					#data = string.replace(data, ' ', '&nbsp;')
					if color_index != -1:
						data = [XMLImage(self.severityMap[color_index], border=0), XMLSpace(2), data]
				else:
					data = formatValue(temp_data, unit_map.get(path))
				csv_data = formatValue(temp_data, unit_map.get(path))
				obj['path_%s' %(path_index_map.get(path))] = data
				obj['_path_%s' %(path_index_map.get(path))] = csv_data
				index += 1
			objs.append(obj)
		"""
		logMsg(2,'objs - %s'%(objs),'makeDataTable')

		width = params.get('width', 845)
		#Get the height based on the width
		if width > 420:
			if len(objs) < 15:
				height = (len(objs) * 16) + 50
			else:
				height = 400
		else:
			height = 400
			# Add dummy entries, so that the table looks better when compare to the others.
			if len(objs) < 15:
				self.addDummyObjs(headings, objs)
		#if len(headings) > 30:
		#	request['no_resource'] = len(headings)
		#elif len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY:
		if len(objs) > maxInputStats.MAX_RECORDS_FOR_HTML_DISPLAY:
			request['no_resource'] = len(objs)
		logMsg(2,'**** objs len - %s'%(len(objs)),'makeDataTable')
		logMsg(2,'**** csv_headings - %s'%(csv_headings),'makeDataTable')
		logMsg(2,'**** headings len - %s'%(len(headings)),'makeDataTable')
		return XMLReportDataAndSummaryTable(headings, objs, data=2, width=width, csv_schema=csv_headings)
	
	def getEventsForInterface(self, resid, start_time, end_time, stat='n_avail'):
		event_tables = self.event_db.getTables(start_time, end_time)
		events = []
		for event_table in event_tables:
			sql_stmt = "select e.fid, e.timeStamp, e.up_down from %s e, tblThresholds t where t.resid IN (%s) and t.statid = %s and e.fid = t.thresid and e.timeStamp between %s and %s order by e.timeStamp ASC"%(event_table, resid, self.rawdn_id_map.get(stat), start_time, end_time)
			#logMsg(2, 'getEventsForInterface : sql_stmt - %s' %(sql_stmt), self.module_name)
			objs = DatabaseServer.executeSQLStatement(sql_stmt)
			#logMsg(2, 'getEventsForInterface : objs length - %s' %(len(objs)), self.module_name)
			if objs != [] and objs != -1:
				events += objs
		if not events:
			events = self.checkForEventsRecursively(resid, start_time, start_time, event_tables[-1])
			if events:
				#However the time of the down event will be considered as the start_time of time time frame selected.
				events[0]['timeStamp'] = start_time
		return events

	def checkForEventsRecursively(self, resid, start_time, tblstart_time, event_table, stat='n_avail'):
		events = []
		#There have been no events within the start and end time,
		#So now need to go backwards to find if any down event occured.
		sql_stmt = "select e.fid, e.timeStamp, e.up_down from %s e, tblThresholds t where t.resid = %d and t.statid = %s and e.fid = t.thresid and e.timeStamp < %s order by e.timeStamp DESC limit 1"%(event_table, resid, self.rawdn_id_map.get(stat), start_time)
		objs = DatabaseServer.executeSQLStatement(sql_stmt)
		if objs and objs!=-1:
			last_event = objs[-1]
			if last_event.get('up_down') == 0:
				#A down event has happened before the time frame selected and needs to be covered.
				events.append(last_event)
		if events:
			return events
		else:
			#we need to go to previous month
			#1st get this month's start
			st_date, et_date = getSpecificMonth(time.strftime('%B, %Y',time.localtime(tblstart_time)))
			month_start_time = timeStrTotimeInt(st_date)
			#get events table: start as one day less so that falls into previous month
			# end is from 1hr from start i.e. just to get the event table for that month
			tblstart_time = safe_int(month_start_time - 86400)
			event_tables = self.event_db.getTables(tblstart_time, tblstart_time+3600)
			if event_tables:
				return self.checkForEventsRecursively(resid, safe_int(month_start_time), tblstart_time, event_tables[-1], stat)
			else:
				return events
		return events

	def getVSATsBasedOnGroup(self, report_dict, group_type='customer'):
		filters=""
		res_query = report_dict.get('res_query')
		if group_type == 'customer':
			customer_platform_resids = {}
			sql_stmt = "SELECT r.*, n.sysContact customer FROM (%s) r, tblNodeInfo n WHERE r.name = n.hostname " %(res_query)
			logMsg(2, 'Get VSATs based on Customer - %s' %sql_stmt, self.module_name)
			res_objs = DatabaseServer.executeSQLStatement(sql_stmt)
			if res_objs and res_objs != -1:
				for res_obj in res_objs:
					if res_obj.get('profile','') == 'vsatdevice.cfg':
						platform, vsatid = string.split(res_obj.get('name', ''), '_')
						resid = safe_int(res_obj.get('resid'))
						customer = res_obj.get('customer', '')
						if not customer_platform_resids.has_key(customer):
							customer_platform_resids[customer] = {}
						if not customer_platform_resids[customer].has_key(platform):
							customer_platform_resids[customer][platform] = {'vsats': []}
						customer_platform_resids[customer][platform]['vsats'].append(resid)
			customers = customer_platform_resids.keys()
			if customers:
				filters="WHERE customer_name IN (%s)" %(str(customers)[1:-1])
			sql_stmt = "SELECT customer_name, platform, bandwidth FROM tblCustomerBandwidth " + filters
			logMsg(2, 'Get Customer - platform bandwidth - %s' %sql_stmt, self.module_name)
			objs = DatabaseServer.executeSQLStatement(sql_stmt)
			if objs and objs != -1:
				for obj in objs:
					customer = obj.get('customer_name', '')
					platform = obj.get('platform', '')
					if not customer_platform_resids.has_key(customer):
						customer_platform_resids[customer] = {}
					if not customer_platform_resids[customer].has_key(platform):
						customer_platform_resids[customer][platform] = {'vsats': []}
					customer_platform_resids[customer][platform]['bandwidth'] = safe_int(obj.get('bandwidth'))
			return customer_platform_resids
		elif group_type == 'platform':
			platform_resids = {}
			logMsg(2, 'Get VSATs based on Platform - %s' %res_query, self.module_name)
			res_objs = DatabaseServer.executeSQLStatement(res_query)
			if res_objs and res_objs != -1:
				for res_obj in res_objs:
					if res_obj.get('profile','') == 'vsatdevice.cfg':
						platform, vsatid = string.split(res_obj.get('name', '_'), '_')
						resid = safe_int(res_obj.get('resid'))
						if not platform_resids.has_key(platform):
							platform_resids[platform] = {'vsats': []}
						platform_resids[platform]['vsats'].append(resid)
			sql_stmt = "SELECT SUM(bandwidth) bandwidth, platform FROM tblCustomerBandwidth GROUP BY platform"
			logMsg(2, 'Get Platform bandwidth - %s' %sql_stmt, self.module_name)
			objs = DatabaseServer.executeSQLStatement(sql_stmt)
			if objs and objs != -1:
				for obj in objs:
					platform = obj.get('platform')
					if not platform_resids.has_key(platform):
						platform_resids[platform] = {'vsats': []}
					platform_resids[platform]['bandwidth'] = safe_int(obj.get('bandwidth'))
			return platform_resids
		else:
			vsat_info = {}
			logMsg(2, 'Get VSATs - %s' %res_query, self.module_name)
			res_objs = DatabaseServer.executeSQLStatement(res_query)
			if res_objs and res_objs != -1:
				for res_obj in res_objs:
					if res_obj.get('profile','') == 'vsatdevice.cfg':
						resid = safe_int(res_obj.get('resid'))
						vsatid = res_obj.get('name')
						vsat_info[vsatid] = resid
			return vsat_info

	def doDownTimeCalulation(self, events, resid, start_time, end_time, vsat_affected_calc=0):
		final_objs = []
		total_downtime = 0
		obj = {}
		for event in events:
			if event.get('up_down') == 0:
				obj['link_down'] = safe_int(event.get('timeStamp'))
			if event.get('up_down') == 1:
				obj['link_up'] = safe_int(event.get('timeStamp'))
				obj['outage'] = safe_int(obj.get('link_up') - obj.get('link_down',0))
			#if both link up and down values are got put into final_objs
			if obj.get('link_down','') and obj.get('link_up',''):
				final_objs.append(obj)
				obj = {}
			elif obj.get('link_up',''):
				obj['link_down'] = start_time
				obj['outage'] = safe_int(obj.get('link_up') - obj.get('link_down',0))
				final_objs.append(obj)
				obj = {}
			elif obj.get('link_down',''):
				obj['link_up'] = ''
				if safe_int(time.time()) < end_time:
					outage_end_time = safe_int(time.time()) + 1
				else :
					outage_end_time = end_time + 1
				obj['outage'] = safe_int(outage_end_time - obj.get('link_down',0))
		if obj:
			final_objs.append(obj)
		if vsat_affected_calc:
			time_vsats_dict = {}
			link_down_times = map(lambda a: a.get('link_down'), final_objs)
			#if any of the link down times is equal to the start time filter it out
			spl_case_time = filter(lambda a: a==start_time, link_down_times)
			link_down_times = filter(lambda a: a!=start_time, link_down_times)
			if link_down_times:
				raw_tables = self.stat_server.rawdb.getTables(start_time, end_time)
				for raw_table in raw_tables:
					sql_stmt = "SELECT timeStamp, avg vsats_affected FROM %s r WHERE r.statid = %s AND resid = %s AND r.timeStamp in (%s)"%(raw_table, self.rawdn_id_map.get('no_of_vsats_up'), resid, str(link_down_times)[1:-1])
					timeObjs = DatabaseServer.executeSQLStatement(sql_stmt)
					if timeObjs and timeObjs!=-1:
						for timeObj in timeObjs:
							cur_time = safe_int(timeObj.get('timeStamp'))
							#if time_vsats_dict.has_key(cur_time):
							#	time_vsats_dict[cur_time] = time_vsats_dict.get(cur_time) + timeObj.get('vsats_affected')
							#else:
							time_vsats_dict.update({cur_time:timeObj.get('vsats_affected')})
					#if all the link down times have got vsats_affected in time_vsats_dict
					if len(link_down_times) == len(time_vsats_dict.keys()):
						break
			#if any of the link down times is equal to the start time
			#no_of_vsats_up shud be found from the last poll that has happened
			if spl_case_time:
				spl_start_time = start_time - 300
				spl_end_time = start_time
				raw_tables = self.stat_server.rawdb.getTables(spl_start_time, spl_end_time-1)
				if raw_tables:
					for raw_table in raw_tables:
						sql_stmt = "SELECT timeStamp, avg vsats_affected FROM %s r WHERE r.statid = %s AND resid = %s AND r.timeStamp BETWEEN %s and %s"%(raw_table,self.rawdn_id_map.get('no_of_vsats_up'), resid, spl_start_time, spl_end_time)
						timeObjs = DatabaseServer.executeSQLStatement(sql_stmt)
						if timeObjs and timeObjs!=-1:
							time_vsats_dict.update({start_time:timeObjs[0].get('vsats_affected')})
							break
			for final_obj in final_objs:
				final_obj['vsats_affected'] = time_vsats_dict.get(final_obj.get('link_down'),'')
		total_downtime = sum(map(lambda a: safe_int(a.get('outage')), final_objs))
		return final_objs, total_downtime

	def getTraps(self, report_dict):
		logMsg(2, "Inside getTraps ", self.module_name)
		db_type = getDBType()
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		top = safe_int(report_dict.get('topn'))
		event_tables = self.trap_db.getTables(start_time, end_time)
		logMsg(2, 'event_tables - %s' %(event_tables), self.module_name)
		resids = report_dict.get('res_path_map', {}).keys()
		stats = report_dict.get('report_obj', {}).get('stats', [])
		trap_msgs = map(lambda a: a.strip(), string.split(stats, '|'))
		time_filter = timeBetween(start_time, end_time, report_dict.get('timebetween',''))
		msg_filter = ''
		if '*' not in trap_msgs:
			#msg_filter = ' AND (%s) '%(string.join(map(lambda a: " msg LIKE '%s'" %(a), trap_msgs), ' OR '))
				trap_messages = trap_msgs[:]
		else:
			trap_messages = []
			if self.resource_manager.profiles.has_key('vsattrapmsg.cfg'):
				trap_messages = map(lambda a: a.get('name', ''), self.resource_manager.profiles['vsattrapmsg.cfg'].getOutputStat())
				
		if trap_messages:
			trap_messages = unique(map(lambda a: a.replace('!!', ';').replace('$$', ','), trap_messages))
			msg_filter = ' AND (%s) '%string.join(map(lambda a: " e.msg ILIKE '%s'" %(a), trap_messages), ' OR ')
		logMsg(2, 'msg_filter %s and trap_messages - %s' %(msg_filter, trap_messages), self.module_name)
		top_str = ''
		if top:
			if db_type == 'MySQL':
				top_str = ' LIMIT %s' %(top)
			elif db_type == 'MSSQL':
				top_str = ' TOP %s ' %(top)
			else:
				top_str = ' LIMIT %s' %(top)

		events = []
		logMsg(2, 'top - %s and top_str - %s' %(top, top_str), self.module_name)
		res_table = report_dict.get('res_query', '')
		# VSAT fluctuated more than 'X' (configurable by user) times 
		occurance = safe_int(report_dict.get('report_obj', {}).get('occurance', 0))
		# making count of the query for each events_tables
		count_entries = 0
		count_entries_dict = {}
		if top_str == '':
			residObjs = []
			residObjs = DatabaseServer.executeSQLStatement(res_table)
			residObjs = map(lambda a :a.get('resid'),residObjs)
			residObjs = listToLine(residObjs)
			
			for event_table in event_tables:
				sql_stmt = 'SELECT count(*) AS countnum FROM %s e WHERE e.timeStamp BETWEEN %s AND %s %s AND e.resid in (%s) %s' %(event_table, start_time, end_time, time_filter,residObjs, msg_filter)
				logMsg(2, "sql_stmt inside loop event_tables -- %s"%sql_stmt, self.module_name)
				objs = DatabaseServer.executeSQLStatement(sql_stmt)
				if objs:
					count_entries_dict[event_table] = safe_int(objs[0].get("countnum", 0))
					count_entries = count_entries + safe_int(objs[0].get("countnum", 0))
		logMsg(2, "count_entries_dict %s -- %s"%(count_entries_dict, count_entries), self.module_name)
		
		query_events_all = []
		for event_table in event_tables:
			temp_table = getUniqueString()
			if occurance:
				sql_stmt = "SELECT * INTO %s FROM (SELECT COUNT(*) event_count, e.resid, e.msg FROM %s e, (%s) r WHERE e.timeStamp BETWEEN %s AND %s  AND e.resid = r.resid %s GROUP BY e.resid, e.msg HAVING COUNT(*) > %s) x"%(temp_table, event_table, res_table, start_time, end_time, msg_filter, occurance)
				logMsg(2, 'occurance sql_stmt - %s' %(sql_stmt), self.module_name)
				DatabaseServer.executeSQLStatement(sql_stmt)
				sql_stmt1 = "SELECT %s e.* FROM %s e, %s t where e.resid = t.resid and e.msg = t.msg ORDER BY e.timeStamp DESC, trapid DESC"%(top_str, event_table, temp_table)
				logMsg(2, 'occurance sql_stmt - %s' %(sql_stmt1), self.module_name)
				objs = DatabaseServer.executeSQLStatement(sql_stmt1)
				logMsg(2, 'occurance objs -- %s' %(len(objs)), self.module_name)
				sql_stmt2 = "DROP TABLE %s"%(temp_table)
				logMsg(2, 'occurance sql_stmt - %s' %(sql_stmt2), self.module_name)
				DatabaseServer.executeSQLStatement(sql_stmt2)
			else:
				#sql_stmt = 'SELECT %s e.* FROM %s e, (%s) r WHERE e.timeStamp BETWEEN %s AND %s %s AND e.resid = r.resid %s ORDER BY e.timeStamp DESC, trapid DESC' %(top_str, event_table, res_table, start_time, end_time, time_filter, msg_filter)
				logMsg(2, 'count_entries - %s' %(count_entries), self.module_name)
				if safe_int(count_entries) > safe_int(maxInputStats.MAX_RECORDS_FOR_REPORTS_DISPLAY):
					logMsg(2, 'maxLimitReached - %s' %(maxInputStats.MAX_RECORDS_FOR_REPORTS_DISPLAY), self.module_name)
					return {"maxLimitReached": 1}
				if 0: # dont know how to handle now --- count_entries > maxInputStats.MAX_RECORDS_FOR_SPLIT_QUERY:
					sql_stmt = 'SELECT %s ROW_NUMBER() OVER(ORDER BY e.timeStamp DESC, trapid DESC) rownum, e.* FROM %s e, (%s) r WHERE e.timeStamp BETWEEN %s AND %s %s AND e.resid = r.resid %s' %(top_str, event_table, res_table, start_time, end_time, time_filter, msg_filter)
					query_events_all.append((sql_stmt, count_entries_dict.get(event_table, 0)))
				else:
					sql_stmt = 'SELECT e.* FROM %s e, (%s) r WHERE e.timeStamp BETWEEN %s AND %s %s AND e.resid = r.resid %s ORDER BY e.timeStamp DESC, trapid DESC %s' %(event_table, res_table, start_time, end_time, time_filter, msg_filter, top_str)
					objs = DatabaseServer.executeSQLStatement(sql_stmt)
				logMsg(2, 'IF ELSE sql_stmt - %s' %(sql_stmt), self.module_name)
			if objs != [] and objs != -1:
				events += objs
			if top and len(events) >= top:
				break
		if query_events_all:
			self.largeReportInProgress = 1
			csv_headings = [
					('severity', 'Severity', 25), 
					('trapid', 'Event ID', 10), 
					('_date', 'Date', 10), 
					('_time', 'Time', 10), 
					('event_code', 'Event Name', 15), 
					('category', 'Element Name', 15), 
					('src_addr', 'IP Address', 15), 
					('eventid', 'Event Code', 10), 
					('msg', 'Event Description', 40),
				]
			titleText = report_dict['report_obj'].get('name', '')
			if report_dict.get('time_period_heading', '') != '':
				titleText = str(titleText) +  " for the Period " + str(report_dict.get('time_period_heading', ''))
			titleTab = report_dict['report_obj'].get('name', '')
			event_codes = newReadAV('vsatconf\\trapcode.ini')
			#event['event_code'] = event_codes.get(str(event.get('eventid')), '')
			report_dict["event_code"] = event_codes
			reportType = "trap"
			report_link = self.getSplitQueryReport(query_events_all, report_dict, csv_headings, titleText, titleTab, reportType)
			self.largeReportInProgress = 0
			return {'report_link' : report_link , 'title' : titleTab, 'isSplitQuery' : 1}
		if top:
			events = events[:top]
		return events

	# function for generating the report based on the split query technique
	def getSplitQueryReport(self, query_to_split, report_dict, csv_headings, titleText, titleTab, reportType):
		try:
			# query_to_split is the list of tuple in [(sqlQuery, query count)....] format
			no_of_records_to_query = maxInputStats.MAX_RECORDS_ONE_SHOT
			splited_query_all = []
			# collecting all the queries
			for query in query_to_split:
				if query[1] > 0:
					sql_stmt = query[0]
					query_count = query[1]
					splited_query = self.getSplitQueryList(sql_stmt, query_count, no_of_records_to_query)
					splited_query_all.extend(splited_query)
			# for quering the data and generating the report one by one by calling the generateSplitQuerySheet function defined in pdfcreator.py
			link = self.unms.pdf_creator.generateSplitQuerySheet(splited_query_all, csv_headings, titleText, titleTab, report_dict, reportType)
			return link
		except Exception, msg:
			logExceptionMsg(4, 'Exception in getSplitQueryReport - %s' %(msg), self.module_name)
			return XMLErrorMessage(msg)

	def getSplitQueryList(self, sql_stmt, query_count, no_of_records_to_query):
		try:	
			no_of_queries = safe_int(math.ceil(safe_float(query_count) / safe_float(no_of_records_to_query)))
			strtRowNo = 0
			endRowNo = no_of_records_to_query
			splited_query = []
			# taking each query and spliting it based on the specified no of records which is taken from no_of_records_to_query
			for i in range(0, no_of_queries):
				strtRowNo = safe_int(no_of_records_to_query * i) + 1
				if i != no_of_queries - 1:
					endRowNo = safe_int(no_of_records_to_query * (i+1))
				else:
					endRowNo = safe_int(query_count)
				sql_stmt_temp = "SELECT * from (%s) as a where rownum between %s and %s"%(sql_stmt, strtRowNo, endRowNo)
				splited_query.append(sql_stmt_temp)
			return 	splited_query
		except Exception, msg:
			logExceptionMsg(4, 'Exception in getSplitQueryList - %s' %(msg), self.module_name)
			return XMLErrorMessage(msg)

	def getDefaultReportOptions(self,request):
		reportas = request.get('reportas', 'detail')
		if not reportas:
			reportas = 'detail'
		request['reportasOptions'] = reportparams.reportAs.items()
		selected_type_list = reportparams.reportas_type_map.get(reportas, ['data'])
		reportdefault = []
		for type in selected_type_list:
			reportdefault.append((type, reportparams.fortigate_report_map[type]))
		request['reportdefaultOptions'] = reportdefault
	
	def makeFortigateGraph(self, sub_report, report_dict):
		report_dict['out'] = {}
		ret = self.getFortigateEvents(sub_report, report_dict)
		reportAs = sub_report.get('stats')
		reportAs = reportAs.split('$')
		reportAs = reportAs[2]
		report_type = report_dict['request'].get('report_type','')
		summary_types = ['pie', 'bar','donut','hbar','trend','area','percentile_area','bar_time','stack_bar_time', 'sq_bar_time']
		if report_type in summary_types:
			reportAs = 'summary'
		if (report_type == 'data'):
			reportAs = 'detail'
		if not ret:
			if(reportAs == 'summary'):
				return self.getFortigateSummaryEvents(sub_report, report_dict)
			if(reportAs == 'detail'):
				return self.getFortigateDataEvents(sub_report, report_dict)
		else:
			return ret

	def getFortigateEvents(self, sub_report, report_dict):
		db_type = getDBType()
		DatabaseServer = getDBInstance()
		start_time = report_dict.get('st')
		end_time = report_dict.get('et')
		report_obj = report_dict.get('report_obj')
		top = safe_int(report_dict.get('topn',report_obj.get('topn')))
		type = safe_int(sub_report.get('group_type'))
		subtype = sub_report.get('stats').split('$')[0]
		logSubtypes = ["System","IPSec","DHCP","PPP","Admin","HA","Auth","Pattern","SMTP Filter","POP3 Filter","IMAP Filter","Infected","Filename","Oversize",
								"Content","Url Filter","Allowed","Blocked","Error","Signature","Anomaly","Allowed","Violation","Alter mail",
								"HTTP","FTP","SMTP","POP3","IMAP","Chassis","dummy","IM","SSL VPN User","SSL VPN Admin","SSL VPN Session",
								"Active X","Cookie","Applet","Other","dummy","SIP","dummy","dummy","Performance","dummy","VIP SSL","LDB Monitor"]
		try:
			subtype = logSubtypes.index(subtype)
		except:
			pass
		filter_string = ""
		resids = report_dict.get('res_path_map', {}).keys()
		filter_string = filter_string + " WHERE e.resid in (%s)"%str(resids)[1:-1]
		if (type == 0):
			filter_string = filter_string +"" 
		else: 
			filter_string = filter_string + " AND e.type = %d"%(type)
		if(subtype == 'All'):
			filter_string = filter_string + ""
		else:
			filter_string = filter_string + " AND e.subtype = %d "%safe_int(subtype)
		filter_string = filter_string + " AND e.timeStamp BETWEEN %s AND %s "%(start_time, end_time)
		fortigate_tables = self.fortigate_db.getTables(start_time, end_time)
		# get all the data from db for the given filter
		events=[]
		for fortigate_table in fortigate_tables:
			sql_stmt = 'SELECT * FROM %s e %s ORDER BY timeStamp'%(fortigate_table,filter_string) 
			objs = DatabaseServer.executeSQLStatement(sql_stmt)
			logMsg(2, 'objs -- %s' %(len(objs)), self.module_name)
			if objs != [] and objs != -1:
				events += objs
		if not events:
			return [XMLSpan('Data Not Available', classId='maptext')]
		# getting top 'n' events for data table based on time
		#events.sort(lambda a, b: safe_int(a.get('timeStamp')) > safe_int(b.get('timeStamp')))
		events.reverse()
		if top:
			top_data_events = events[:top]
		else:
			top_data_events = events
		report_dict['top_data_events'] = top_data_events
		# For Summary
		# classify the data based on selected classify column
		classifyBy = sub_report.get('stats')
		classifyBy = classifyBy.split('$')
		classifyBy = classifyBy[1]
		classify_events = classifyObjs(events, lambda a : a.get(classifyBy, ""))
		log_subtypes = {'All':'All',0:"System",1:"IPSec",2:"DHCP",3:"PPP",4:"Admin",5:"HA",6:"Auth",7:"Pattern",8:"SMTP Filter",9:"POP3 Filter",10:"IMAP Filter",11:"Infected",12:"Filename",13:"Oversize",
				14:"Content",15:"Url Filter",16:"Allowed",17:"Blocked",18:"Error",19:"Signature",20:"Anomaly",21:"Allowed",22:"Violation",23:"Alter mail",
				24:"HTTP",25:"FTP",26:"SMTP",27:"POP3",28:"IMAP",29:"Chassis",31:"IM",32:"SSL VPN User",33:"SSL VPN Admin",34:"SSL VPN Session",
				35:"Active X",36:"Cookie",37:"Applet",38:"Other",40:"SIP",43:"Performance",45:"VIP SSL",46:"LDB Monitor"}
		# getting the top count data from the result
		summary_events = []
		for key, value in classify_events.items():
			if(classifyBy == 'subtype'):
				obj = {'key':log_subtypes[key], 'value': len(value), 'actual_key': key}
			else:
				obj = {'key':key, 'value': len(value), 'actual_key': key}
			summary_events.append(obj)
		summary_events.sort(lambda a, b: a.get('value') > b.get('value'))
		if top:
			top_events = summary_events[:top]
		else:
			top_events = summary_events
		# For Time based Charts		
		top_events_data = {}
		for obj in top_events:
			top_events_data[obj['key']] = classify_events[obj['actual_key']]
		report_dict['summary_events'] = top_events
		report_dict['all_events'] = top_events_data

	def getFortigateSummaryEvents(self, sub_report, report_dict):
		summary_events = report_dict.get('summary_events', [])
		data = map(lambda a: (a.get('key', ''), a.get('value')), summary_events)
		report_dict['out']['data'] = data
		report_dict['out']['data_count'] = len(data)
		#report_dict['report_type'] = sub_report['reporttype']
		report_type = report_dict['request'].get('report_type',sub_report.get('reporttype'))
		report_dict['report_type'] = report_type
		summary_report_types = ['pie', 'bar','donut','hbar']
		report_type = report_dict.get('report_type', 'summary')
		if report_type == 'summary':
			return self.makeFortigateSummaryTable(summary_events, sub_report, report_dict)
		elif report_type in summary_report_types:
			return [self.drawGraph(report_dict)]
		else:
			return self.getTimeBasedSummaryReport(sub_report, report_dict)

	def makeFortigateSummaryTable(self, events, sub_report, report_dict):
		"""Draw the summary table.
		"""
		classifyBy = sub_report.get('stats')
		classifyBy = classifyBy.split('$')
		classifyBy = classifyBy[1]
		classDict = {'subtype':'Sub Type','src_ip':'Source IP','timeStamp':'Time','service':'Service','dest_ip':'Destination IP'}
		classifyBy = classDict.get(classifyBy,'')
		headings = [('key', classifyBy, '50%', 'center'), ('value', 'Count', '50%', 'center')]
		width = report_dict.get('width', 420)
		if width > 420:
			if len(events) < 15:
				height = (len(events) * 16) + 50
			else:
				height = 280
		else:
			height = 280
		return [XMLReportDataAndSummaryTable(headings, events, font_size=report_dict.get('font', 's'), data=1, width=width)]

	def getFortigateDataEvents(self, sub_report, report_dict):
		"""Make Events Table
		"""
		events = report_dict.get('top_data_events', [])
		priorityMap = {
				0: 'images/alarm_indicator_1.gif', 1: 'images/alarm_indicator_2.gif', 
				2: 'images/alarm_indicator_2.gif', 3: 'images/alarm_indicator_3.gif', 
				4: 'images/alarm_indicator_3.gif', 5 : 'images/alarm_indicator_4.gif', 
				6: 'images/alarm_indicator_5.gif',
			}
		log_types = {0:'All', 1:'Event',2: 'Antivirus',3:'Web Filter',4: 'Attack',
							5: 'Spam Filter',6:'Content Archive',7 :'IM',8 :'VOIP'}
		log_subtypes = {'All':'All',0:"System",1:"IPSec",2:"DHCP",3:"PPP",4:"Admin",5:"HA",6:"Auth",7:"Pattern",8:"SMTP",9:"POP3",10:"IMAP",11:"Infected",12:"Filename",13:"Oversize",
						14:"Content",15:"Url Filter",16:"Allowed",17:"Blocked",18:"Error",19:"Signature",20:"Anomaly",21:"Allowed",22:"Violation",23:"Alter mail",
						24:"HTTP",25:"FTP",26:"SMTP",27:"POP3",28:"IMAP",29:"Chassis",31:"IM",32:"SSL VPN User",33:"SSL VPN Admin",34:"SSL VPN Session",
						35:"Active X",36:"Cookie",37:"Applet",38:"Other",40:"SIP",43:"Performance",45:"VIP SSL",46:"LDB Monitor"}
		res_path_map = report_dict.get('res_path_map')
		headings = [('priority', 'Status', '5%', 'center'), ('timeStamp', 'TimeStamp', '20%', 'center'), ('type', 'SyslogType', '5%', 'left'),('subtype', 'SyslogsubType', '5%', 'left'),('sourceip', 'SourceIP', '5%', 'left'),('destip', 'DestIP', '5%', 'left'),('msg', 'Description', '30%', 'left')]
		objs = []
		for event in events:
			#path = res_path_map.get(safe_int(event.get('resid')))
			#stat_name = changeOption(self.statid_name_map.get(safe_int(event.get('statid'))))
			#path_name = '%s ' %(path)
			msg = event.get('msg', '')
			type= event.get('type','')
			subtype=event.get('subtype','')
			sourceip=event.get('src_ip','')
			destip=event.get('dest_ip','')
			obj = {}
			obj['priority'] = XMLImage(priorityMap.get(safe_int(event.get('priority')), 1))
			obj['timeStamp'] = time.strftime('%d-%b-%Y %H:%M:%S', time.localtime(safe_int(event.get('timeStamp'))))
			obj['type'] = XMLSpan(log_types.get(type), title=type)
			obj['msg'] = XMLSpan(msg, title=msg)
			obj['subtype']=XMLSpan(log_subtypes.get(subtype), title=subtype)
			obj['sourceip']=XMLSpan(sourceip, title=sourceip)
			obj['destip']=XMLSpan(destip, title=destip)
			objs.append(obj)
		width = report_dict.get('width', 420)
		if width > 420:
			if len(objs) < 15:
				height = (len(objs) * 16) + 50
			else:
				height = 280
		else:
			height = 280
		return [XMLReportDataAndSummaryTable(headings, objs, font_size=report_dict.get('font', 's'), data=1, width=width)]
	

def getCurrentWeekDay(time_stamp):
	time_tup = time.localtime(time_stamp)
	return calendar.weekday(time_tup[0],time_tup[1],time_tup[2])

def getTodayStartTimeStamp(time_stamp):
	time_tup = time.localtime(time_stamp)
	return time_stamp - (time_tup[3]*60*60 + time_tup[4]*60 + time_tup[5])

def getTotalDurationForBusinessHr(start_time, end_time, business_hr_map):
	first_time  = 0
	total_duration = 0
	day_0th_time = getTodayStartTimeStamp(start_time)
	while (day_0th_time < end_time):
		weekday = getCurrentWeekDay(day_0th_time)
		# print time.ctime(day_0th_time), weekday, ">>>"
		bus_st_time, bus_et_time = business_hr_map.get(weekday, [0,0])
		if safe_int(bus_st_time) and safe_int(bus_et_time):
			bus_st_time = safe_int(bus_st_time)
			bus_et_time = safe_int(bus_et_time) + 60 #Adding 60 seconds
		else:
			bus_st_time = safe_int(bus_st_time)
			bus_et_time = safe_int(bus_et_time) #Dont add 60 seconds
		if first_time == 0:
			if (start_time - day_0th_time) > bus_st_time:
				  bus_st_time = start_time - day_0th_time 
				  # print bus_st_time, ">>>bus_st_time", bus_et_time - bus_st_time
		if (day_0th_time+86400 > end_time):
			if (day_0th_time + bus_et_time)	> end_time:
				bus_et_time = end_time - day_0th_time
				# print bus_et_time, ">>>bus_et_time", bus_et_time - bus_st_time
		first_time += 1
		day_0th_time += 86400
		# print bus_st_time, bus_et_time, ">>>----------->>" , bus_et_time - bus_st_time , weekday
		total_duration += (bus_et_time - bus_st_time)
	return total_duration
	#print st, time.ctime(st)

def makeTimeDeltaHour(t) :
	#return time in x days, y hours, z mins, u seconds
	out = ""
	hours = t/3600
	t = t%3600
	mins = t/60
	t = t%60
	secs = t
	start = 0
	if hours:
		out = out + "%d hr, "%hours
		start = 1
	if mins or start :
		out = out + "%d min, "%mins
		start = 1
	out = out + "%d sec"%secs
	return out

