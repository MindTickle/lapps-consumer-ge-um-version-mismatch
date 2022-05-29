package ge_version_mismatch

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
	"time"
)

func GEVersionMismatchFix() {

	ctx := context.Background()

	conn, err := grpc.Dial("tdb-svc-sqlsvc.internal-grpc.prod.mindtickle.com:80", grpc.WithDefaultCallOptions(), grpc.WithInsecure())
	if err != nil {
		mtlog.Fatalf(nil, "Error connecting with sql service %+v", err)
		return
	}

	sqlStoreHelper := mtstore_helper.NewSqlStoreClient(conn, "automate-migration", "prod")

	batchSize := 300
	tenantIdList := []string{"920348052064774113"}
	for _, tenantId := range tenantIdList {
		mtlog.Infof(ctx, "tenant: %v", tenantId)
		start := 208000

		for {
			t1 := time.Now()
			sqlQuery := fmt.Sprintf("select tenant_id, company_id, user_id, entity_id, entity_version, entity_type from user_entity where tenant_id= %d order by id ASC limit %d offset %d", utils.StrTo(tenantId).OrgIdToInt64WithoutError(), batchSize, start)
			mtlog.Infof(ctx, "at row %v", start)
			docs, err := sqlStoreHelper.SearchRows(ctx, &mtstore_helper.SearchRowRequest{
				TableName: "user_entity",
				Query:     &mtstore_helper.SQLQueryString{Query: sqlQuery},
			}, common.RequestMeta{
				OrgId:     tenantId,
				CompanyId: "",
				App:       "migration",
			})

			if err != nil {
				mtlog.Fatalf(nil, "Error connecting with doc service %+v", err)
				return
			}

			if docs == nil || len(docs) == 0 {
				mtlog.Warn(ctx, "no docs found")
				docs = []*tickleDbSqlStore.SqlRow{}
			}

			if len(docs) < batchSize {
				break
			}

			mtlog.Infof(ctx, "query time: %v", time.Since(t1))

			userEntities, err := GetUserEntityFromSqlStoreDocs(docs)
			if err != nil {
				mtlog.Error(ctx, "failed to marshall req error: %s", err)
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
					mtlog.Errorf(ctx, "error whilw fetching data %v", err)
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

			//bulkInitReq := make([]*platform_client.BulkInitRequest,0)
			execQueries := make([]mtstore_helper.ExecRequest, 0)
			for elKey, platformData := range platformELData {

				if geData, exists := lappsUserEntities[elKey]; exists {
					if platformData.EntityVersion != geData.Version {
						mtlog.Infof(ctx, "Got data issue %v %v", platformData.CompanyId, elKey)

						execQuery := GetUpdateQuery(tenantId, platformData.Id(), geData.Version)
						execQueries = append(execQueries, mtstore_helper.ExecRequest{
							Query: mtstore_helper.SQLQueryString{
								Query: execQuery,
							},
							TableName: "user_entity",
						})
					}
				}
			}

			if len(execQueries) > 0 {
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
	return fmt.Sprintf("update user_entity set entity_version = %d where tenant_id = %d and id = '%s'",
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
