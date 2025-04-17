using AutoMapper;
using cube.api.eAxis.BL.Business.Common;
using cube.api.eAxis.BL.Business.Helper;
using cube.api.eAxis.DTO;
using cube.api.eAxis.DTO.ProFX.Flexi;
using cube.api.eAxis.DTO.ProFX.RBAC;
using cube.api.eAxis.DTO.ProTrust.Common;
using cube.api.eAxis.REPO.Factory;
using cube.api.eAxis.REPO.Helpers;
using cube.api.eAxis.UI;
using cube.api.eAxis.UI.ProFX.Flexi;
using cube.api.eAxis.UI.ProTrust.Common;
using Cube.Fx.Bl.ProFX.Flexi;
using EllipticCurve.Utils;
using Lib.Helper.Extensions;
using Lib.Helper.FileGeneration;
using Lib.MultiLog.NLogs;
using Newtonsoft.Json;
using SendGrid;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Web.UI.WebControls;
using System.Windows.Input;
using cube.api.eAxis.UI.DMS;
using Lib.Helper.CommonHelpers;
using Newtonsoft.Json.Linq;
using Cube.Fx.Helper.CommonHelpers;
using cube.api.eAxis.BL.Business.DMS;
using System.Web;
using cube.api.eAxis.UI.Org;

namespace cube.api.eAxis.BL.Business.ProFX.Flexi
{
    public class FlexiReportBL
    {
        public readonly string strFilterID = "FLXRT";
        FindFilterListBL objFindFilterListBL;
        Predicate objPredicate;
        private ILoggingService _loggingService;
        public FlexiReportBL()
        {
            objFindFilterListBL = new FindFilterListBL();
            objPredicate = new Predicate();
            _loggingService = LoggingService.GetLoggingService();
        }
        public APIResponse Insert(UIFlexiReport objUIFlexiReport)
        {
            var apiResponse = new APIResponse();
            var _Instance = TrustFactory.GetAppInstance("TC");
            try
            {
                if (objUIFlexiReport.PK != Guid.Empty)
               {
                    FlexiReportValidations validator = IsValidInsert(objUIFlexiReport);
                    if (validator.IsValid)
                    {
                        FlexiReport objFlexiReport = new FlexiReport();
                        objFlexiReport = UItoDTO(objUIFlexiReport);
                        objFlexiReport.FLX_CreatedDateTime = System.DateTime.UtcNow;
                        objFlexiReport.FLX_ModifiedDateTime = System.DateTime.UtcNow;
                        var resultSet = _Instance.Insert(objFlexiReport);
                        objUIFlexiReport = DTOtoUI(resultSet);
                        apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                    }
                    else
                    {
                        apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, "Could Not Save");
                        apiResponse.Id = 1;
                        apiResponse.Status = Status.Failed;
                        apiResponse.Response = objUIFlexiReport;
                        apiResponse.Count = 0;
                        apiResponse.Validations = validator.Errors;
                        return apiResponse;
                    }
                }
                else
                {
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, "GUID is Mandatory");
                    apiResponse.Id = 1;
                    apiResponse.Status = Status.Failed;
                    apiResponse.Response = objUIFlexiReport;
                    apiResponse.Count = 0;
                    return apiResponse;
                }
                apiResponse.Id = 1;
                apiResponse.Status = Status.Success;
                apiResponse.Response = objUIFlexiReport;
                apiResponse.Count = 1;
                return apiResponse;  
            }
            catch (Exception ex)
            {
                _loggingService.Error(ex);
                apiResponse.Status = Status.Failed;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APIFailed);
                return apiResponse;
            }
            finally
            {
                _Instance.Dispose(); System.GC.SuppressFinalize(this);
            }
        }
        public FlexiReportValidations IsValidInsert(UIFlexiReport objUIFlexiReport, bool IsUpdate = false)
        {
            FlexiReportValidations validator = new FlexiReportValidations();
            validator.IsUpdate = IsUpdate;
            validator.objUIFlexiReport = objUIFlexiReport;
            validator.AddObjToValidate("UIFlexiReport", objUIFlexiReport);
            validator.Validate();
            return validator;
        }
        public APIResponse Update(UIFlexiReport objUIFlexiReport)
        {
            var apiResponse = new APIResponse();
            var _Instance = TrustFactory.GetAppInstance("TC");
            try
            { 
                FlexiReportValidations validator = IsValidInsert(objUIFlexiReport, true);
                if (validator.IsValid)
                {
                    FlexiReport objFlexiReport = new FlexiReport();
                    objFlexiReport = UItoDTO(objUIFlexiReport);
                    objFlexiReport.FLX_ModifiedDateTime = System.DateTime.UtcNow;
                    var resultSet = _Instance.Update(objFlexiReport);
                    apiResponse.Response = DTOtoUI(resultSet);
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.UpdateSuccess);
                }
                else
                {
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, "Update Failed");
                    apiResponse.Id = 1;
                    apiResponse.Status = Status.Failed;
                    apiResponse.Response = objUIFlexiReport;
                    apiResponse.Count = 0;
                    apiResponse.Validations = validator.Errors;
                    return apiResponse;
                }
                apiResponse.Id = 1;
                apiResponse.Count = 1;
                apiResponse.Status = Status.Success;
                return apiResponse;
            }
            catch (Exception ex)
            {
                _loggingService.Error(ex);
                apiResponse.Status = Status.Failed;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APIFailed);
                return apiResponse;
            }
            finally
            {
                _Instance.Dispose(); System.GC.SuppressFinalize(this);
            }
        }
        public APIResponse Delete(string strPk)
        {
            var apiResponse = new APIResponse();
            var _Instance = TrustFactory.GetAppInstance("TC");
            try
            {
                var resultSet = string.Empty;
                UIStatus objUIStatus = new UIStatus();
                List<FilterListDetails> FilterList = objFindFilterListBL.GenericExpressionforPk(strFilterID, "FLX_PK", strPk);
                Expression<Func<vwFlexiReport, bool>> query = objPredicate.PredicateFilterSearch<vwFlexiReport>(FilterList);
                var GetMany = _Instance.GetMany(query).FirstOrDefault();
                if (GetMany != null)
                {
                    resultSet = _Instance.Delete(GetMany);
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.DeleteSuccess);
                }
                else
                {
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, "Could not delete");
                    apiResponse.Status = Status.Failed;
                    return apiResponse;
                }
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.DeleteSuccess);
                objUIStatus.Status = resultSet.ToString();
                apiResponse.Response = objUIStatus.Status;
                apiResponse.Count = 1;
                apiResponse.Status = Status.Success;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                return apiResponse;
            }
            catch (Exception ex)
            {
                _loggingService.Error(ex);
                apiResponse.Status = Status.Failed;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APIFailed);
                return apiResponse;
            }
            finally
            {
                _Instance.Dispose(); System.GC.SuppressFinalize(this);
            }
        }
        public APIResponse GetById(string listPK)
        {
            var apiResponse = new APIResponse();
            var _Instance = TrustFactory.GetAppInstance("TC");
            try
            {
                List<FilterListDetails> FilterList = objFindFilterListBL.GenericExpressionDataType(strFilterID, "FLX_PK", listPK);
                Expression<Func<vwFlexiReport, bool>> query = objPredicate.PredicateFilterSearch<vwFlexiReport>(FilterList);
                var GetMany = _Instance.GetMany(query).ToList();
                apiResponse.Response = ListvwDTOtoUI(GetMany).FirstOrDefault();
                apiResponse.Count = 1;
                apiResponse.Status = Status.Success;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                return apiResponse;
            }
            catch (Exception ex)
            {
                _loggingService.Error(ex);
                apiResponse.Status = Status.Failed;
                apiResponse.Error = ex.Message;
                apiResponse.Exception = ex;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APIFailed);
                return apiResponse;
            }
            finally
            {
                _Instance.Dispose(); System.GC.SuppressFinalize(this);
            }
        }
        public APIResponse GetMany(FilterInput lstPackagesInput)
        {
            var _Instance = TrustFactory.GetAppInstance("TC");
            List<UIFlexiReport> Result = new List<UIFlexiReport>();
            var apiResponse = new APIResponse();
            try
            {
                Mapper.CreateMap<UIInput, Input>();
                List<Input> lstobj = Mapper.Map<List<UIInput>, List<Input>>(lstPackagesInput.SearchInput);
                var objPaging = TrustHelper.GetPaging(lstobj);
                Expression<Func<vwFlexiReport, bool>> query = objPredicate.PredicateFilterSearch<vwFlexiReport>(TrustHelper.RemovePaging(objFindFilterListBL.FindFilter(lstPackagesInput)), IsTenantEnabled: false, IsBaseTenantEnabled: false);
                List<vwFlexiReport> lstFlexiReport = new List<vwFlexiReport>();
                if (objPaging.PageNumber == 0)
                    lstFlexiReport = _Instance.GetMany(query).ToList();
                else
                    lstFlexiReport = _Instance.GetMany(query, objPaging.PageNumber, objPaging.PageSize, objPaging.SortColumn, objPaging.SortType).ToList();
                apiResponse.Id = 1;
                apiResponse.Status = Status.Success;
                apiResponse.Response = ListvwDTOtoUI(lstFlexiReport).ToList();
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                apiResponse.Count = _Instance.GetManyCount(query);
                return apiResponse;
            }
            catch (Exception ex)
            {
                _loggingService.Error(ex);
                apiResponse.Status = Status.Failed;
                apiResponse.Error = ex.Message;
                apiResponse.Exception = ex;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APIFailed);
                return apiResponse;
            }
            finally
            {
                _Instance.Dispose(); System.GC.SuppressFinalize(this);
            }
        }
        public APIResponse GetPreviewData(string strPk)
        {
            var _Instance = TrustFactory.GetAppInstance("RPT_R");
            var apiResponse = new APIResponse();
            try
            {
                string query = string.Empty;
                string totalCountQuery = string.Empty;
                string resultJSON = string.Empty;
                int maxRowsToFetch = 0;
                var dataTable = new DataTable();
                List<UIFlexiReport> lstUIFlexiReport = new List<UIFlexiReport>();
                UIFlexiReport objUIFlexiReport = new UIFlexiReport();                
                FilterInput AppsettingFilterProd = new FilterInput();
                AppsettingFilterProd.FilterID = "FLXRT";
                AppsettingFilterProd.SearchInput = new List<UIInput>() { new UIInput() { FieldName = "PK", Value = strPk.ToString() } };
                APIResponse objapi = GetMany(AppsettingFilterProd);
                if (objapi != null && objapi.Response != null)
                {
                    lstUIFlexiReport = (List<UIFlexiReport>)objapi.Response;
                    objUIFlexiReport = lstUIFlexiReport.FirstOrDefault<UIFlexiReport>();
                    bool isIntParsable = int.TryParse(ConfigurationManager.AppSettings["FlexiMaxRowsToFetch"], out maxRowsToFetch);
                    if(!isIntParsable || maxRowsToFetch <= 0)
                    {
                        maxRowsToFetch = 20;
                    }
                    query = GenerateQuery(objUIFlexiReport, maxRowsToFetch: maxRowsToFetch);
                    totalCountQuery = GenerateQuery(objUIFlexiReport, isForAllCount: true);   
                }
                if (!string.IsNullOrEmpty(query))
                {
                    try
                    {
                        dataTable = _Instance.ExecuteQuery(query);
                        if (dataTable != null && dataTable.Rows.Count > 0 )
                        {
                            apiResponse.Id = dataTable.Rows.Count;
                            apiResponse.Response = dataTable;
                            objUIFlexiReport.PreviewStatus = "Previewed";
                            Update(objUIFlexiReport);
                            if(dataTable.Rows.Count == maxRowsToFetch)
                            {
                                DataTable dt = _Instance.ExecuteQuery(totalCountQuery);
                                if (dt != null && dt.Rows.Count > 0) 
                                    apiResponse.Count = int.Parse(dt.Rows[0][0].ToString());
                            }
                            else 
                                apiResponse.Count = dataTable.Rows.Count;
                            apiResponse.Status = Status.Success;
                            apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                        }
                        else
                        {
                            apiResponse.Status = Status.Success;
                            apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, "No Data found for the defined query.");
                        }
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                else
                {
                    apiResponse.Status = Status.Failed;
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, "Query formation issue. Check if Select columns, View name, Where conditions present in your Flexi Report config.");
                }
                apiResponse.Status = Status.Success;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                return apiResponse;
            }
            catch (Exception ex)
            {
                _loggingService.Error(ex);
                apiResponse.Status = Status.Failed;
                apiResponse.Error = ex.Message;
                apiResponse.Exception = ex;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APIFailed);
                return apiResponse;
            }
            finally
            {
                _Instance.Dispose(); System.GC.SuppressFinalize(this);
            }

        }
        public APIResponse ExportToCsvAsByteArray(string strPk)
        {
            var _Instance = TrustFactory.GetAppInstance("RPT_R");
            var apiResponse = new APIResponse();
            try
            {
                string query = string.Empty;
                FlexiExcelGenerationData objFlexiExcelGenerationData = new FlexiExcelGenerationData();
                GoogleUploadInput Gout = new GoogleUploadInput();
                string fileName = string.Empty;
                List<UIFlexiReport> lstUIFlexiReport = new List<UIFlexiReport>();
                UIFlexiReport objUIFlexiReport = new UIFlexiReport();
                FilterInput AppsettingFilterProd = new FilterInput();
                AppsettingFilterProd.FilterID = "FLXRT";
                AppsettingFilterProd.SearchInput = new List<UIInput>() { new UIInput() { FieldName = "PK", Value = strPk.ToString() } };
                APIResponse objapi = GetMany(AppsettingFilterProd);
                if (objapi != null && objapi.Response != null)
                {
                    lstUIFlexiReport = (List<UIFlexiReport>)objapi.Response;
                    objUIFlexiReport = lstUIFlexiReport.FirstOrDefault<UIFlexiReport>();
                    query = GenerateQuery(objUIFlexiReport);
                    objFlexiExcelGenerationData.headers = objUIFlexiReport.SelectColumns.Split(',').Distinct().ToList();
                    objFlexiExcelGenerationData.reportName = objUIFlexiReport.ReportName;
                    objFlexiExcelGenerationData.reportGeneratedDateAndTime = DateTime.UtcNow.ToString();
                    fileName = lstUIFlexiReport.FirstOrDefault<UIFlexiReport>().ReportName ?? "temp";
                }
                if (!string.IsNullOrEmpty(query))
                {
                    Gout = GetRecordsAsByteArray(query, fileName, objFlexiExcelGenerationData);
                    apiResponse.Response = Gout;
                }
                apiResponse.Id = 1;
                apiResponse.Status = Status.Success;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                return apiResponse;
            }
            catch (Exception ex)
            {
                _loggingService.Error(ex);
                apiResponse.Status = Status.Failed;
                apiResponse.Error = ex.Message;
                apiResponse.Exception = ex;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APIFailed);
                return apiResponse;
            }
            finally
            {
                _Instance.Dispose(); System.GC.SuppressFinalize(this);
            }
        }
        public APIResponse ExportToCsvAsByteArray(FilterInput filterInput)
        {
            var apiResponse = new APIResponse();
            string query = string.Empty;
            GoogleUploadInput Gout = new GoogleUploadInput();
            string fileName = string.Empty;
            List<UIFlexiReport> lstUIFlexiReport = new List<UIFlexiReport>();
            UIFlexiReport objUIFlexiReport = new UIFlexiReport();
            FilterInput AppsettingFilterProd = new FilterInput();
            try
            {
                if (filterInput != null && filterInput.SearchInput != null)
                {
                    string strPk = TrustHelper.GetPropertyName(filterInput, "FLX_PK");
                    string orgInfoStr = TrustHelper.GetPropertyName(filterInput, "OrgInfo");
                    AppsettingFilterProd.FilterID = "FLXRT";
                    FlexiExcelGenerationData objFlexiExcelGenerationData = new FlexiExcelGenerationData();

                    AppsettingFilterProd.SearchInput = new List<UIInput>() { new UIInput() { FieldName = "PK", Value = strPk?.ToString() } };
                    APIResponse objapi = GetMany(AppsettingFilterProd);
                    if (objapi != null && objapi.Response != null)
                    {
                        lstUIFlexiReport = (List<UIFlexiReport>)objapi.Response;
                        objUIFlexiReport = lstUIFlexiReport.FirstOrDefault<UIFlexiReport>();
                        if (objUIFlexiReport != null)
                        {
                            objFlexiExcelGenerationData.headers = objUIFlexiReport.SelectColumns?.Split(',').Distinct().ToList();
                            objFlexiExcelGenerationData.reportName = objUIFlexiReport.ReportName;
                        }
                        else
                        {
                            apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, "The given input is null", MsgType.UpdateFailed.ToString());
                        }
                        objFlexiExcelGenerationData.reportGeneratedDateAndTime = DateTime.UtcNow.ToString();
                        query = GenerateQuery(objUIFlexiReport, isScheduler: true, orgInfoStr: orgInfoStr);
                        fileName = lstUIFlexiReport?.FirstOrDefault<UIFlexiReport>().ReportName ?? "temp";
                    }
                    if (!string.IsNullOrEmpty(query) && !string.IsNullOrEmpty(fileName) && objFlexiExcelGenerationData != null)
                    {
                        Gout = GetRecordsAsByteArray(query, fileName, objFlexiExcelGenerationData);
                        apiResponse.Response = Gout;
                    }
                    apiResponse.Status = Status.Success;
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                }
                else
                {
                    apiResponse.Status = Status.Failed;
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, "The given input is null", MsgType.APIFailed.ToString());
                }
                apiResponse.Id = 1;
                return apiResponse;
            }
            catch (Exception ex)
            {
                _loggingService.Error(ex);
                apiResponse.Status = Status.Failed;
                apiResponse.Error = ex.Message;
                apiResponse.Exception = ex;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APIFailed);
                return apiResponse;
            }
            finally
            {
                System.GC.SuppressFinalize(this);
            }
        }
        public APIResponse GetViewList(string ColumnsFor)
        {
            var apiResponse = new APIResponse();
            var _Instance = TrustFactory.GetAppInstance("RPT_R");
            List<string> lstViews = new List<string>();
            try
            {
                string queryToFetchAllViews = string.Empty;
                object jsonValue = null;
                ConfigHelper configHelper = new ConfigHelper(ConfigFileIndex.FlexiConfig);
                bool isKeyExist = configHelper.TryGetValue("SqlQueries.QueryToFetchAllViews", out jsonValue);
                if(isKeyExist && jsonValue != null) 
                {
                    queryToFetchAllViews = jsonValue.ToString();
                }
                else
                {
                    throw new Exception("Required Key or Json does not exist(for query formation).");
                }
                if (!string.IsNullOrEmpty(queryToFetchAllViews))
                {
                    DataTable dt = _Instance.ExecuteQuery(queryToFetchAllViews);
                    var allViewsStr = JsonConvert.SerializeObject(dt);
                    var allViews = JsonConvert.DeserializeObject<dynamic>(allViewsStr);
                    foreach (var view in allViews)
                    {
                        lstViews.Add(view.name.ToString());
                    }
                    if (lstViews.Count > 0)
                    {
                        apiResponse.Id = 1;
                        apiResponse.Response = lstViews;
                        apiResponse.Count = lstViews.Count;
                        apiResponse.Status = Status.Success;
                        apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                    }
                    else
                    {
                        apiResponse.Id = 1;
                        apiResponse.Status = Status.Failed;
                        apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, "No Views in the Database");
                        apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                    }
                }
                else
                {
                    throw new Exception("Required query is null or empty");
                }
                return apiResponse;
            }
            catch (Exception ex)
            {
                _loggingService.Error(ex);
                apiResponse.Status = Status.Failed;
                apiResponse.Error = ex.Message;
                apiResponse.Exception = ex;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APIFailed);
                return apiResponse;
            }
            finally
            {
                _Instance.Dispose(); System.GC.SuppressFinalize(this);
            }
        }
        public APIResponse GetColumnsName(string viewName)
        {
            var apiResponse = new APIResponse();
            var _Instance = TrustFactory.GetAppInstance("RPT_R");
            List<string> listcolumns = new List<string>();
            try
            {
                string queryToFetchAllViewColumns = $"SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('dbo.{viewName}');";
                DataTable dt = _Instance.ExecuteQuery(queryToFetchAllViewColumns);
                var allColumnsStr = JsonConvert.SerializeObject(dt);
                var allColumns = JsonConvert.DeserializeObject<dynamic>(allColumnsStr);
                foreach (var column in allColumns)
                {
                    listcolumns.Add("[" + column.name.ToString() + "]");
                }
                if (listcolumns.Count > 0)
                {
                    apiResponse.Id = 1;
                    apiResponse.Response = listcolumns;
                    apiResponse.Count = listcolumns.Count;
                    apiResponse.Status = Status.Success;
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                }
                else
                {
                    apiResponse.Id = 1;
                    apiResponse.Status = Status.Failed;
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, "View does not exist");
                    apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APISuccess);
                }
                return apiResponse;
            }
            catch (Exception ex)
            {
                _loggingService.Error(ex);
                apiResponse.Status = Status.Failed;
                apiResponse.Error = ex.Message;
                apiResponse.Exception = ex;
                apiResponse.Messages = Messages.GetMessage(apiResponse.Messages, MsgType.APIFailed);
                return apiResponse;
            }
            finally
            {
                _Instance.Dispose(); System.GC.SuppressFinalize(this);
            }
        }
        private string GenerateQuery(UIFlexiReport objUIFlexiReport, bool isScheduler = false, string orgInfoStr = null, int maxRowsToFetch = 0, bool isForAllCount = false)
        {
            string query = string.Empty;
            if (objUIFlexiReport != null)
            {
                string selectedColumns = objUIFlexiReport.SelectColumns, viewName = objUIFlexiReport.TableOrView, whereCondition = objUIFlexiReport.WhereClassData, orderBy = objUIFlexiReport.OrderByData;
                if (!string.IsNullOrEmpty(selectedColumns) && !string.IsNullOrEmpty(viewName) && !string.IsNullOrEmpty(whereCondition))
                {
                    if (isScheduler)
                    {
                        if (!string.IsNullOrEmpty(orgInfoStr))
                        {
                            string orgQuery = string.Empty;
                            List<OrgInfo> lstObjOrgInfos = JsonConvert.DeserializeObject<List<OrgInfo>>(orgInfoStr);
                            foreach (OrgInfo orgInfo in lstObjOrgInfos)
                            {
                                if (!string.IsNullOrEmpty(orgInfo.OrgColumnName) && !string.IsNullOrEmpty(orgInfo.OrgColumnValues))
                                    orgQuery = orgInfo.OrgColumnName + " IN ('" + string.Join("','", orgInfo.OrgColumnValues.Split(',')) + "')";
                            }
                            if (!string.IsNullOrEmpty(orgQuery))
                                query = $"SELECT {selectedColumns} FROM {viewName} WHERE ({whereCondition}) AND {orgQuery};";
                            else
                                query = $"SELECT {selectedColumns} FROM {viewName} WHERE ({whereCondition});";
                        }
                        else
                        {
                            query = $"SELECT {selectedColumns} FROM {viewName} WHERE ({whereCondition});";
                        }
                    }
                    else if (maxRowsToFetch > 0)
                    {
                        query = $"SELECT TOP {maxRowsToFetch} {selectedColumns} FROM {viewName} WHERE ({whereCondition});";
                    }
                    else if (isForAllCount)
                    {
                        query = $"SELECT COUNT(0) AS TotalCount FROM {viewName} WHERE ({whereCondition});";
                    }
                    else
                    {
                        query = $"SELECT {selectedColumns} FROM {viewName} WHERE ({whereCondition});";
                    }
                    if (!isForAllCount && (!string.IsNullOrEmpty(orderBy) || !string.IsNullOrWhiteSpace(orderBy)))
                    {
                        query = query.Remove(query.Length - 1);
                        query += $" ORDER BY {orderBy};";
                    }
                }
            }
            return query;
        }
        private GoogleUploadInput GetRecordsAsByteArray(string query, string fileName, FlexiExcelGenerationData objFlexiExcelGenerationData = null)
        {
            var dataTable = new DataTable();
            GoogleUploadInput GoutAsCSV = new GoogleUploadInput();

            var _Instance = TrustFactory.GetAppInstance("RPT_R");

            try
            {
                // Execute the query and get the results
                dataTable = _Instance.ExecuteQuery(query);

                // Check if dataTable has rows
                //if (dataTable == null || dataTable.Rows.Count == 0) // allow the reports to download with the report name and column
                //{
                //    return null;
                //}

                // Initialize the configuration helper
                ConfigHelper configHelper = new ConfigHelper(ConfigFileIndex.CommonConfig);

                // Retrieve configuration values
                if (configHelper.TryGetValue("FlexiReportTemplate.Templatepath", out object tempPath) &&
                    configHelper.TryGetValue("FlexiReportTemplate.TemplateName", out object tempName))
                {
                    string workingFolder = HttpRuntime.AppDomainAppPath + tempPath.ToString() + tempName.ToString();
                    objFlexiExcelGenerationData.templatePath = workingFolder;
                }
                else
                {
                    throw new Exception("Configuration values for template path or name are missing.");
                }

                // Convert DataTable to byte array
                byte[] excelBytes = CSVFileExtensions.ToExcelByteArray(dataTable, objFlexiExcelGenerationData);

                // Prepare GoogleUploadInput
                GoutAsCSV.Data = excelBytes;
                GoutAsCSV.Name = !string.IsNullOrEmpty(fileName) ? fileName + ".xlsx" : "temp.xlsx";
                GoutAsCSV.Type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
                GoutAsCSV.DocType = "excel";
                GoutAsCSV.Base64str = Convert.ToBase64String(excelBytes);

                return GoutAsCSV;
            }
            catch (SqlException sqlEx)
            {
                throw new Exception("Custom Exception: "+ sqlEx.Message);
            }
            catch (Exception ex)
            {
                // Log or handle the specific exception message and stack trace
                // Replace with appropriate logging mechanism
                throw new Exception("Failed to Export CsvAsByteArray: " + ex.Message, ex);
            }
            finally
            {
                _Instance.Dispose();
            }
        }
        public FlexiReport UItoDTO(UIFlexiReport objUIFlexiReport)
        {
            FlexiReport objFlexiReport = new FlexiReport();
            objFlexiReport.FLX_PK = objUIFlexiReport.PK;
            objFlexiReport.FLX_Module = objUIFlexiReport.Module;
            objFlexiReport.FLX_SubModule = objUIFlexiReport.SubModule;
            objFlexiReport.FLX_ReportName = objUIFlexiReport.ReportName;
            objFlexiReport.FLX_Description = objUIFlexiReport.Description;
            objFlexiReport.FLX_TableOrView = objUIFlexiReport.TableOrView;
            objFlexiReport.FLX_SelectColumns = objUIFlexiReport.SelectColumns;
            objFlexiReport.FLX_WhereClassData = objUIFlexiReport.WhereClassData;
            objFlexiReport.FLX_OrderByData = objUIFlexiReport.OrderByData;
            objFlexiReport.FLX_ReportCode = objUIFlexiReport.ReportCode;
            objFlexiReport.FLX_Party = objUIFlexiReport.Party;
            objFlexiReport.FLX_ColNameForOrg = objUIFlexiReport.ColNameForOrg;
            objFlexiReport.FLX_CreatedBy = objUIFlexiReport.CreatedBy;
            objFlexiReport.FLX_CreatedDateTime = objUIFlexiReport.CreatedDateTime;
            objFlexiReport.FLX_ModifiedBy = objUIFlexiReport.ModifiedBy;
            objFlexiReport.FLX_ModifiedDateTime = objUIFlexiReport.ModifiedDateTime;
            objFlexiReport.CMN_TenantCode = objUIFlexiReport.CMN_TenantCode;
            objFlexiReport.FLX_PreviewStatus = objUIFlexiReport.PreviewStatus;
            return objFlexiReport;
        }
        public UIFlexiReport DTOtoUI(FlexiReport objFlexiReport)
        {
            UIFlexiReport objUIFlexiReport = new UIFlexiReport();
            objUIFlexiReport.PK = objFlexiReport.FLX_PK;
            objUIFlexiReport.Module = objFlexiReport.FLX_Module;
            objUIFlexiReport.SubModule = objFlexiReport.FLX_SubModule;
            objUIFlexiReport.ReportName = objFlexiReport.FLX_ReportName;
            objUIFlexiReport.Description = objFlexiReport.FLX_Description;
            objUIFlexiReport.TableOrView = objFlexiReport.FLX_TableOrView;
            objUIFlexiReport.SelectColumns = objFlexiReport.FLX_SelectColumns;
            objUIFlexiReport.WhereClassData = objFlexiReport.FLX_WhereClassData;
            objUIFlexiReport.OrderByData = objFlexiReport.FLX_OrderByData;
            objUIFlexiReport.ReportCode = objFlexiReport.FLX_ReportCode;
            objUIFlexiReport.Party = objFlexiReport.FLX_Party;
            objUIFlexiReport.ColNameForOrg = objFlexiReport.FLX_ColNameForOrg;
            objUIFlexiReport.CreatedBy = objFlexiReport.FLX_CreatedBy;
            objUIFlexiReport.CreatedDateTime = objFlexiReport.FLX_CreatedDateTime;
            objUIFlexiReport.ModifiedBy = objFlexiReport.FLX_ModifiedBy;
            objUIFlexiReport.ModifiedDateTime = objFlexiReport.FLX_ModifiedDateTime;
            objUIFlexiReport.CMN_TenantCode = objFlexiReport.CMN_TenantCode;
            objUIFlexiReport.PreviewStatus = objFlexiReport.FLX_PreviewStatus;
            return objUIFlexiReport;
        }
        public List<UIFlexiReport> listDTOtoUI(List<FlexiReport> lstFlexiReport)
        {
            List<UIFlexiReport> lstUIFlexiReport = new List<UIFlexiReport>();
            foreach (FlexiReport objFlexiReport in lstFlexiReport)
            {
                UIFlexiReport objUIFlexiReport = new UIFlexiReport();
                objUIFlexiReport.PK = objFlexiReport.FLX_PK;
                objUIFlexiReport.Module = objFlexiReport.FLX_Module;
                objUIFlexiReport.SubModule = objFlexiReport.FLX_SubModule;
                objUIFlexiReport.ReportName = objFlexiReport.FLX_ReportName;
                objUIFlexiReport.Description = objFlexiReport.FLX_Description;
                objUIFlexiReport.TableOrView = objFlexiReport.FLX_TableOrView;
                objUIFlexiReport.SelectColumns = objFlexiReport.FLX_SelectColumns;
                objUIFlexiReport.WhereClassData = objFlexiReport.FLX_WhereClassData;
                objUIFlexiReport.OrderByData = objFlexiReport.FLX_OrderByData;
                objUIFlexiReport.ReportCode = objFlexiReport.FLX_ReportCode;
                objUIFlexiReport.Party = objFlexiReport.FLX_Party;
                objUIFlexiReport.ColNameForOrg = objFlexiReport.FLX_ColNameForOrg;
                objUIFlexiReport.CreatedBy = objFlexiReport.FLX_CreatedBy;
                objUIFlexiReport.CreatedDateTime = objFlexiReport.FLX_CreatedDateTime;
                objUIFlexiReport.ModifiedBy = objFlexiReport.FLX_ModifiedBy;
                objUIFlexiReport.ModifiedDateTime = objFlexiReport.FLX_ModifiedDateTime;
                objUIFlexiReport.CMN_TenantCode = objFlexiReport.CMN_TenantCode;
                objUIFlexiReport.PreviewStatus = objFlexiReport.FLX_PreviewStatus;
                lstUIFlexiReport.Add(objUIFlexiReport);
            }
            return lstUIFlexiReport;
        }
        public List<UIFlexiReport> ListvwDTOtoUI(List<vwFlexiReport> lstFlexiReport)
        {
            List<UIFlexiReport> lstUIFlexiReport = new List<UIFlexiReport>();
            foreach (vwFlexiReport objFlexiReport in lstFlexiReport)
            {
            UIFlexiReport objUIFlexiReport = new UIFlexiReport();
            objUIFlexiReport.PK = objFlexiReport.FLX_PK;
            objUIFlexiReport.Module = objFlexiReport.FLX_Module;
            objUIFlexiReport.SubModule = objFlexiReport.FLX_SubModule;
            objUIFlexiReport.ReportName = objFlexiReport.FLX_ReportName;
            objUIFlexiReport.Description = objFlexiReport.FLX_Description;
            objUIFlexiReport.TableOrView = objFlexiReport.FLX_TableOrView;
            objUIFlexiReport.SelectColumns = objFlexiReport.FLX_SelectColumns;
            objUIFlexiReport.WhereClassData = objFlexiReport.FLX_WhereClassData;
            objUIFlexiReport.OrderByData = objFlexiReport.FLX_OrderByData;
            objUIFlexiReport.ReportCode = objFlexiReport.FLX_ReportCode;
            objUIFlexiReport.Party = objFlexiReport.FLX_Party;
            objUIFlexiReport.ColNameForOrg = objFlexiReport.FLX_ColNameForOrg;
            objUIFlexiReport.CreatedBy = objFlexiReport.FLX_CreatedBy;
            objUIFlexiReport.CreatedDateTime = objFlexiReport.FLX_CreatedDateTime;
            objUIFlexiReport.ModifiedBy = objFlexiReport.FLX_ModifiedBy;
            objUIFlexiReport.ModifiedDateTime = objFlexiReport.FLX_ModifiedDateTime;
            objUIFlexiReport.CMN_TenantCode = objFlexiReport.CMN_TenantCode;
            objUIFlexiReport.PreviewStatus = objFlexiReport.FLX_PreviewStatus;
            lstUIFlexiReport.Add(objUIFlexiReport);
            }
            return lstUIFlexiReport;
        }
        public UIFlexiReport PrepareDefaultFlexiReportPKandCode()
        {
            try
            {
                UIFlexiReport objUIFlexiReport = new UIFlexiReport();
                AppCounterBL objAppCounterBL = new AppCounterBL();
                objUIFlexiReport.PK = Guid.NewGuid();
                objUIFlexiReport.ReportCode = "RPT-" + objAppCounterBL.GetNextSequence("FlexiReport");
                return objUIFlexiReport;
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
    public class OrgInfo
    {
        public string OrgColumnName { get; set; }
        public string OrgColumnValues { get; set; }
    }
}
