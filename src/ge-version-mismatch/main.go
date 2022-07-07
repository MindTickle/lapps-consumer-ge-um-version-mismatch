package main

import (
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
const UpdateEntityVersionUserAssessmentActivityQuery = "update user_assessment_activity set entity_version = %d where tenant_id = %d and id = '%s'"
const UpdateEntityVersionUserChecklistActivityQuery = "update user_checklist_activity set entity_version = %d where tenant_id = %d and id = '%s'"
const UpdateEntityVersionUserCourseActivityQuery = "update user_course_activity set entity_version = %d where tenant_id = %d and id = '%s'"
const UpdateEntityVersionUserIltActivityQuery = "update user_ilt_activity set entity_version = %d where tenant_id = %d and id = '%s'"
const UpdateEntityVersionUserQuickUpdateActivityQuery = "update user_qu_activity set entity_version = %d where tenant_id = %d and id = '%s'"
const UpdateEntityVersionUserReinforcementActivityQuery = "update user_reinforcement_activity set entity_version = %d where tenant_id = %d and id = '%s'"

func main() {

	tenantIdList := []string{"598851564828422091", "601750695889109690", "603064054389485772", "603230645817339328", "606786330229378423",
		"606807873465789366", "608161145422037655", "609230816894535496", "610382636479437300", "610730375189137708"}

	mtlog.Init(&mtlog.Config{
		Level:      mtlog.LogLevelDebug,
		OutputPath: []string{"stdout", fmt.Sprintf("application-%v-07-2022-tenantId-%s.log", time.Now().Day(), tenantIdList[0])},
	})

	ctx := context.Background()

	track := "prod"
	serviceHost := "tdb-svc-sqlsvc.internal-grpc.prod.mindtickle.com"

	conn, err := grpc.Dial(serviceHost+":80", grpc.WithDefaultCallOptions(), grpc.WithInsecure())
	if err != nil {
		mtlog.Fatalf(nil, "Error connecting with sql service %+v", err)
		return
	}

	sqlStoreHelper := mtstore_helper.NewSqlStoreClient(conn, "automate-ge-version-mismatch", track)

	batchSize := 300

	for _, tenantId := range tenantIdList {
		mtlog.Infof(ctx, "starting for tenantId %v", tenantId)
		start := 0

		for {
			t1 := time.Now()
			sqlQuery := fmt.Sprintf(FetchDataByTenantId, utils.StrTo(tenantId).OrgIdToInt64WithoutError(), batchSize, start)
			mtlog.Infof(ctx, "starting at row %v", start)
			docs, err := sqlStoreHelper.SearchRows(ctx, &mtstore_helper.SearchRowRequest{
				TableName: "user_entity",
				Query:     &mtstore_helper.SQLQueryString{Query: sqlQuery},
			}, common.RequestMeta{
				OrgId:     tenantId,
				CompanyId: "",
				App:       "ge-version-mismatch",
			})

			if err != nil {
				mtlog.Fatalf(nil, "Error connecting with doc service  %+v failed for start %d tenantId %s", err, start, tenantId)
				return
			}

			if docs == nil || len(docs) == 0 {
				mtlog.Warn(ctx, "no docs found")
				docs = []*tickleDbSqlStore.SqlRow{}
				break
			}

			mtlog.Infof(ctx, "query time: %v for start %d tenantId %s ", time.Since(t1), start, tenantId)

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
					if utils.Int64ToDecimalStr(activity.EntityId) == "1216675537468414263" && utils.Int64ToHexStr(activity.UserId) == "498f5e687b5c85e5" {
						mtlog.Infof(ctx, "found the required user and entity for userEntity platformELData formation")
					}
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
				resp, err := getDataInParallel(ctx, userModules, companyId)
				mtlog.Errorf(ctx, "getDataInParallel resp %s tenantId %s", resp, tenantId)
				if err != nil {
					mtlog.Errorf(ctx, "error while fetching data %v tenantId %s in batchSize %s", err, tenantId, batchSize)
					return
				}

				for _, geSummary := range resp {
					if geSummary.GamificationEntityId == "1216675537468414263" && geSummary.UserId == "498f5e687b5c85e5" {
						mtlog.Infof(ctx, "found the required user and entity for geSummary")
					}
					lappsUserEntities[GetEntityLearnerKey(geSummary.GamificationEntityId, geSummary.UserId)] = geSummary
				}
				mtlog.Infof(ctx, "ge summary time: %v", time.Since(t2))
			}

			mtlog.Debugf(ctx, "platformELData : %s", platformELData)
			mtlog.Debugf(ctx, "len(platformELData) = %v", len(platformELData))

			mtlog.Debugf(ctx, "lappsUserEntities : %s", lappsUserEntities)

			execQueries := make([]mtstore_helper.ExecRequest, 0)
			for elKey, platformData := range platformELData {

				if geData, exists := lappsUserEntities[elKey]; exists {
					mtlog.Debugf(ctx, "platformData.EntityVersion = %v and geData.Version = %v", platformData.EntityVersion, geData.Version)
					if "1216675537468414263" == utils.Int64ToDecimalStr(platformData.EntityId) && utils.Int64ToHexStr(platformData.UserId) == "498f5e687b5c85e5" {
						mtlog.Infof(ctx, "found the required user and entity for version mismatch check")
						mtlog.Infof(ctx, "platformData.EntityVersion = %v and geData.Version = %v", platformData.EntityVersion, geData.Version)
					}
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
				mtlog.Debugf(ctx, "Query : %s", execQueries)
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
	mtlog.Infof(ctx, "script ended")
}

func getDataInParallel(ctx context.Context, userModules []UserModule, companyId int64) ([]*pojos.GESummaryESObject, error) {
	errs, ctx := errgroup.WithContext(ctx)
	batches := getBatches(userModules)
	resp := make([]*pojos.GESummaryESObject, 0)

	for index := range batches {
		batch := batches[index]
		errs.Go(func() error {
			var err error
			data, err := getGESummaryData(ctx, batch, companyId)
			if err != nil {
				mtlog.Errorf(ctx, "error while fetching accessible module in series %v", err)
				return err
			}
			resp = append(resp, data...)
			return err
		})
		time.Sleep(1 * time.Second)
	}

	if err := errs.Wait(); err != nil {
		return nil, err
	}
	return resp, nil
}

func getGESummaryData(ctx context.Context, userModules []UserModule, companyId int64) ([]*pojos.GESummaryESObject, error) {
	var userEntityGeSummaries = make([]*pojos.GESummaryESObject, 0)
	for index := range userModules {
		batch := userModules[index]
		url := GetGEUrlForEntityData(batch.UserId, batch.EntityId, companyId)
		resp, err := http.Get(url)
		mtlog.Infof(ctx, "url of ge data: %s", url)
		mtlog.Debugf(ctx, "Resp of ge data : %s", resp)
		if err != nil {
			mtlog.Errorf(ctx, "Error in getting game summary err: %s. request body : %s", err, resp)
			return nil, err
		} else if resp.StatusCode == http.StatusNotFound {
			mtlog.Warningf(ctx, "StatusNotFound userId %s companyId %s entityId %s", batch.UserId, companyId, batch.EntityId)
			continue
		} else if resp.StatusCode != http.StatusOK {
			mtlog.Errorf(ctx, "non 200 status code %s for game engine for user entities. request body : %s", resp.StatusCode, resp)
			return nil, err
		}
		gebytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			mtlog.Errorf(ctx, "error in reading from response body")
		}

		var geSummary *pojos.GESummaryESObject
		_ = json.Unmarshal(gebytes, &geSummary)
		userEntityGeSummaries = append(userEntityGeSummaries, geSummary)
	}
	return userEntityGeSummaries, nil
}

func getBatches(modules []UserModule) [][]UserModule {
	opsLength := len(modules)
	var splitted [][]UserModule
	batchSize := 5
	for start := 0; start < opsLength; start += batchSize {
		end := start + batchSize
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

	return fmt.Sprintf(UpdateEntityVersionUserCourseActivityQuery,
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserQuickUpdateActivityQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf(UpdateEntityVersionUserQuickUpdateActivityQuery,
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserAssessmentActivityQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf(UpdateEntityVersionUserAssessmentActivityQuery,
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserChecklistActivityQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf(UpdateEntityVersionUserChecklistActivityQuery,
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserIltActivityQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf(UpdateEntityVersionUserIltActivityQuery,
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func UpdateUserReinforcementActivityQuery(tenantId string, rowId string, correctVersion int64) string {
	return fmt.Sprintf(UpdateEntityVersionUserReinforcementActivityQuery,
		correctVersion,
		utils.StrTo(tenantId).OrgIdToInt64WithoutError(),
		rowId)
}

func isLAEntity(activity *UserEntityDbModel) bool {
	laEntities := []string{"UPDATE", "COURSE", "ASSESSMENT", "CHECKLIST", "ILT", "REINFORCEMENT"}
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

func GetGEUrlForEntityData(userId string, entityId string, companyId int64) string {
	return "http://gen-svc-ge-init-bulk.internal.prod.mindtickle.com/user/" + userId + "/company/" + utils.CnameToStringWithoutError(companyId) + "/ge/" + entityId
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
