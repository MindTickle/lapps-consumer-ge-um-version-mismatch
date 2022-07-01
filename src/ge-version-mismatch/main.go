package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/MindTickle/ge_summary_migration/pojos"
	"github.com/MindTickle/infracommon/mtlog"
	"github.com/MindTickle/platform-common/mtstore_helper"
	"github.com/MindTickle/platform-common/utils"
	"github.com/MindTickle/platform-protos/pb/common"
	"github.com/MindTickle/storageprotos/pb/tickleDbSqlStore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

const UpdateUserEntityVersionQuery = "update user_entity set entity_version = %d where tenant_id = %d and id = '%s'"
const FetchDataByTenantId = "select tenant_id, company_id, user_id, entity_id, entity_version, entity_type from user_entity " +
	"where tenant_id= %d order by id ASC limit %d offset %d"

func main() {

	mtlog.Init(&mtlog.Config{
		Level:      mtlog.LogLevelInfo,
		OutputPath: []string{"stdout", "application.log"},
	})

	ctx := context.Background()

	track := "staging"
	serviceHost := "tdb-svc-sqlsvc.internal-grpc.staging.mindtickle.com"

	conn, err := grpc.Dial(serviceHost+":80", grpc.WithDefaultCallOptions(), grpc.WithInsecure())
	if err != nil {
		mtlog.Fatalf(nil, "Error connecting with sql service %+v", err)
		return
	}

	sqlStoreHelper := mtstore_helper.NewSqlStoreClient(conn, "automate-ge-version-mismatch", track)

	batchSize := 300
	tenantIdList := []string{"1215511379423111306"}
	for _, tenantId := range tenantIdList {
		mtlog.Infof(ctx, "starting for tenantId %v", tenantId)
		start := 0

		for {
			t1 := time.Now()
			sqlQuery := fmt.Sprintf(FetchDataByTenantId, utils.StrTo(tenantId).OrgIdToInt64WithoutError(), batchSize, start)
			mtlog.Infof(ctx, "at row %v", start)
			docs, err := sqlStoreHelper.SearchRows(ctx, &mtstore_helper.SearchRowRequest{
				TableName: "user_entity",
				Query:     &mtstore_helper.SQLQueryString{Query: sqlQuery},
			}, common.RequestMeta{
				OrgId:     tenantId,
				CompanyId: "",
				App:       "ge-version-mismatch",
			})

			if err != nil {
				mtlog.Fatalf(nil, "Error connecting with doc service  %+v failed for tenantId %s", err, tenantId)
				return
			}

			if docs == nil || len(docs) == 0 {
				mtlog.Warn(ctx, "no docs found")
				docs = []*tickleDbSqlStore.SqlRow{}
				break
			}

			mtlog.Infof(ctx, "query time: %v for tenantId %s ", time.Since(t1), tenantId)

			userEntities, err := GetUserEntityFromSqlStoreDocs(docs)
			if err != nil {
				mtlog.Error(ctx, "failed to marshall req error: %s tenantId %s", err, tenantId)
				err := status.Error(codes.Internal, "")
				mtlog.Error(ctx, err)
				return
			}

			companyWiseActivityDocs := make(map[int64][]*UserEntityDbModel, 0)
			lappsUserEntities := make(map[string]*pojos.GESummaryESObject, 0)
			platformELData := make(map[string]*UserEntityDbModel, 0)

			for _, activity := range userEntities {
				if isLAEntity(activity) {
					platformELData[GetEntityLearnerKey(utils.Int64ToDecimalStr(activity.EntityId), utils.Int64ToHexStr(activity.UserId))] = activity

					if _, ok := companyWiseActivityDocs[activity.CompanyId]; !ok {
						companyWiseActivityDocs[activity.CompanyId] = []*UserEntityDbModel{}
					}
					companyWiseActivityDocs[activity.CompanyId] = append(companyWiseActivityDocs[activity.CompanyId], activity)
				}
			}

			userModules := make([]UserModule, 0)

			for companyId, activityDocs := range companyWiseActivityDocs {
				for _, entityLearner := range activityDocs {
					userModules = append(userModules, UserModule{
						UserId:   utils.Int64ToHexStr(entityLearner.UserId),
						EntityId: utils.Int64ToDecimalStr(entityLearner.EntityId),
					})
				}

				t2 := time.Now()
				resp, err := getDataInParallel(ctx, userModules, companyId, tenantId)
				if err != nil {
					mtlog.Errorf(ctx, "error while fetching data %v", err)
				}

				//requestBody := ReinforcementRequestObject{UserModules: userModules}
				//jsonValue, _ := json.Marshal(requestBody)
				//resp, err := http.Post(GetGameEngineReinforcementUrl(tenantId, companyId), "application/json", bytes.NewBuffer(jsonValue))
				//if err != nil {
				//	mtlog.Errorf(ctx, "Error in making post request to game engine err: %s. request body : %s", err, string(jsonValue))
				//} else if resp.StatusCode != http.StatusOK {
				//	mtlog.Errorf(ctx, "non 200 status code for game engine for reinforcement. request body : %s", string(jsonValue))
				//}
				//gebytes, err := ioutil.ReadAll(resp.Body)
				//if err != nil {
				//	mtlog.Errorf(ctx, "error in reading from response body")
				//}

				for _, geSummary := range resp {
					lappsUserEntities[GetEntityLearnerKey(geSummary.GamificationEntityId, geSummary.UserId)] = geSummary
				}
				mtlog.Infof(ctx, "ge summary time: %v", time.Since(t2))
			}

			execQueries := make([]mtstore_helper.ExecRequest, 0)
			for elKey, platformData := range platformELData {

				if geData, exists := lappsUserEntities[elKey]; exists {
					if platformData.EntityVersion != geData.Version {
						mtlog.Infof(ctx, "Got data issue %v %v tenantId %s companyId %s", platformData.CompanyId,
							elKey, tenantId, platformData.CompanyId)

						execQuery := GetUpdateQuery(tenantId, platformData.Id(), geData.Version)
						execQueries = append(execQueries, mtstore_helper.ExecRequest{
							Query: mtstore_helper.SQLQueryString{
								Query: execQuery,
							},
							TableName: "user_entity",
						})
					}

					userEntityDataId := fmt.Sprintf("%s|%s|%s",
						strconv.FormatInt(platformData.UserId, 16),
						strconv.FormatInt(platformData.CompanyId, 10),
						strconv.FormatInt(platformData.EntityId, 10))
					//6 table update "UPDATE", "COURSE", "ASSESSMENT", "CHECKLIST", "ILT", "REINFORCEMENT"
					switch {
					case platformData.EntityType == "UPDATE":
						execQuery := UpdateUserQuickUpdateActivityQuery(tenantId, userEntityDataId, geData.Version)
						execQueries = append(execQueries, mtstore_helper.ExecRequest{
							Query: mtstore_helper.SQLQueryString{
								Query: execQuery,
							},
							TableName: "user_qu_activity",
						})
					case platformData.EntityType == "COURSE":
						execQuery := UpdateUserCourseActivityQuery(tenantId, userEntityDataId, geData.Version)
						execQueries = append(execQueries, mtstore_helper.ExecRequest{
							Query: mtstore_helper.SQLQueryString{
								Query: execQuery,
							},
							TableName: "user_course_activity",
						})
					case platformData.EntityType == "ASSESSMENT":
						execQuery := UpdateUserAssessmentActivityQuery(tenantId, userEntityDataId, geData.Version)
						execQueries = append(execQueries, mtstore_helper.ExecRequest{
							Query: mtstore_helper.SQLQueryString{
								Query: execQuery,
							},
							TableName: "user_assessment_activity",
						})
					case platformData.EntityType == "CHECKLIST":
						execQuery := UpdateUserChecklistActivityQuery(tenantId, userEntityDataId, geData.Version)
						execQueries = append(execQueries, mtstore_helper.ExecRequest{
							Query: mtstore_helper.SQLQueryString{
								Query: execQuery,
							},
							TableName: "user_checklist_activity",
						})
					case platformData.EntityType == "ILT":
						execQuery := UpdateUserIltActivityQuery(tenantId, userEntityDataId, geData.Version)
						execQueries = append(execQueries, mtstore_helper.ExecRequest{
							Query: mtstore_helper.SQLQueryString{
								Query: execQuery,
							},
							TableName: "user_ilt_activity",
						})
					case platformData.EntityType == "REINFORCEMENT":
						execQuery := UpdateUserReinforcementActivityQuery(tenantId, userEntityDataId, geData.Version)
						execQueries = append(execQueries, mtstore_helper.ExecRequest{
							Query: mtstore_helper.SQLQueryString{
								Query: execQuery,
							},
							TableName: "user_reinforcement_activity",
						})
					}
				}
			}

			if len(execQueries) > 0 {
				mtlog.Infof(ctx, "Executing query tenantId %s", tenantId)
				mtlog.Infof(ctx, "Query : %s", execQueries)
				execResp, err := sqlStoreHelper.Exec(ctx, &execQueries, common.RequestMeta{
					OrgId:           tenantId,
					CompanyId:       "",
					App:             "data-correction",
					Authorizer:      "data-correction",
					GlobalContextId: "data-correction",
				})

				if err != nil {
					mtlog.Error(ctx, "error in update query %v", err)
					return
				}
				mtlog.Infof(ctx, "update resp %v", execResp)
			}
			start = start + batchSize
		}
	}
}

func getDataInParallel(ctx context.Context, userModules []UserModule, companyId int64, tenantId string) ([]*pojos.GESummaryESObject, error) {
	errs, ctx := errgroup.WithContext(ctx)
	batches := getBatches(userModules)
	resp := make([]*pojos.GESummaryESObject, 0)

	for index := range batches {
		batch := batches[index]
		errs.Go(func() error {
			var err error
			data, err := getGESummaryData(ctx, batch, companyId, tenantId)
			if err != nil {
				mtlog.Errorf(ctx, "error while fetching accessible module in series %v", err)
				return err
			}
			resp = append(resp, data...)
			return err
		})
	}

	if err := errs.Wait(); err != nil {
		return nil, err
	}
	return resp, nil
}

func getGESummaryData(ctx context.Context, userModules []UserModule, companyId int64, tenantId string) ([]*pojos.GESummaryESObject, error) {
	requestBody := LappsGetUserEntitiesRequestObject{UserModules: userModules}
	jsonValue, _ := json.Marshal(requestBody)

	resp, err := http.Post(GetGEUrlForActivityData(tenantId, companyId), "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		mtlog.Errorf(ctx, "Error in making post request to game engine err: %s. request body : %s", err, string(jsonValue))
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		mtlog.Errorf(ctx, "non 200 status code for game engine for user entities. request body : %s", string(jsonValue))
		return nil, err
	}
	gebytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		mtlog.Errorf(ctx, "error in reading from response body")
	}
	var userEntityGeSummaries []*pojos.GESummaryESObject
	_ = json.Unmarshal(gebytes, &userEntityGeSummaries)
	return userEntityGeSummaries, nil
}

func getBatches(modules []UserModule) [][]UserModule {
	opsLength := len(modules)
	var splitted [][]UserModule
	for start := 0; start < opsLength; start += 30 {
		end := start + 30
		if end > opsLength {
			end = opsLength
		}
		splitted = append(splitted, modules[start:end])
	}
	return splitted
}

func GetUpdateQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf(UpdateUserEntityVersionQuery,
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserCourseActivityQuery(tenantId string, rowId string, correctVersion int64) string {

	return fmt.Sprintf("update user_course_activity set entity_version = %d where tenant_id = %d and id = '%s'",
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserQuickUpdateActivityQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf("update user_qu_activity set entity_version = %d where tenant_id = %d and id = '%s'",
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserAssessmentActivityQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf("update user_assessment_activity set entity_version = %d where tenant_id = %d and id = '%s'",
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserChecklistActivityQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf("update user_checklist_activity set entity_version = %d where tenant_id = %d and id = '%s'",
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserIltActivityQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf("update user_ilt_activity set entity_version = %d where tenant_id = %d and id = '%s'",
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserReinforcementActivityQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf("update user_reinforcement_activity set entity_version = %d where tenant_id = %d and id = '%s'",
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func isLAEntity(activity *UserEntityDbModel) bool {
	laEntities := []string{"UPDATE", "COURSE", "ASSESSMENT", "CHECKLIST", "ILT"}
	for _, entityType := range laEntities {
		if activity.EntityType == entityType {
			return true
		}
	}
	return false
}

func GetEntityLearnerKey(entityId string, userId string) string {
	return entityId + "|" + userId
}

func GetGEUrlForActivityData(orgId string, companyId int64) string {
	return "http://ge.internal.mindtickle.com/org/" + orgId + "/company/" + utils.CnameToStringWithoutError(companyId) + "/getUsersGEVOs"
}

func GetGameEngineReinforcementUrl(orgId string, companyId int64) string {
	return "http://ge.internal.mindtickle.com/org/" + orgId + "/company/" + utils.CnameToStringWithoutError(companyId) + "/reinforcement/getUserModules"
}

func GetUserEntityFromSqlStoreDocs(docs []*tickleDbSqlStore.SqlRow) ([]*UserEntityDbModel, error) {
	users := make([]*UserEntityDbModel, len(docs))
	for id, userDoc := range docs {
		user := &UserEntityDbModel{}
		stringMap := map[string]string{}
		ConvertMapByteToMapString(userDoc.Data, stringMap)
		data, err := json.Marshal(stringMap)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(data, user)
		if err != nil {
			return nil, err
		}
		users[id] = user
	}
	return users, nil
}

func ConvertMapByteToMapString(byteMap map[string][]byte, stringMap map[string]string) {
	for key, val := range byteMap {
		stringMap[key] = string(val)
	}
}
