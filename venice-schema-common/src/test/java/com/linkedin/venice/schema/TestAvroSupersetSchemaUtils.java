package com.linkedin.venice.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.AvroSupersetSchemaUtils;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAvroSupersetSchemaUtils {

  @Test
  public void testGenerateSupersetSchemaFromValueSchemasWithTwoSchemas() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\","
        + "\"fields\":"
        + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"}"
        + " ]"
        + "}";

    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\","
        + "\"fields\":"
        + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"company\",\"type\":\"string\", \"default\" : \"linkedin\"}"
        + " ]"
        + "}";

    SchemaEntry schemaEntry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry schemaEntry2 = new SchemaEntry(2, schemaStr2);

    SchemaEntry supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
        Arrays.asList(schemaEntry1, schemaEntry2)
    );

    final Schema expectedSupersetSchema = AvroSchemaUtils.generateSuperSetSchema(schemaEntry1.getSchema(), schemaEntry2.getSchema());
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(expectedSupersetSchema, supersetSchemaEntry.getSchema()));
    Assert.assertEquals(supersetSchemaEntry.getId(), 2);

    supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
        Arrays.asList(schemaEntry2, schemaEntry1) // Order should not matter.
    );
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(expectedSupersetSchema, supersetSchemaEntry.getSchema()));
    Assert.assertEquals(supersetSchemaEntry.getId(), 2);

    // Test the case where generated superset schema entry should have schema ID 1 instead of 2.
    schemaEntry1 = new SchemaEntry(2, schemaStr1);
    schemaEntry2 = new SchemaEntry(1, schemaStr2);

    supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
        Arrays.asList(schemaEntry2, schemaEntry1) // Order should not matter.
    );
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(expectedSupersetSchema, supersetSchemaEntry.getSchema()));
    Assert.assertEquals(supersetSchemaEntry.getId(), 1);
  }

  @Test
  public void testGenerateSupersetSchemaFromThreeValueSchemas() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," + "\"fields\":" + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"}" + " ]" + "}";

    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," + "\"fields\":" + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"company\",\"type\":\"string\", \"default\" : \"linkedin\"}"
        // New field compared to schemaStr1
        + " ]" + "}";

    String schemaStr3 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," + "\"fields\":" + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"age\",\"type\":\"int\", \"default\" : 29}" // Another new field compared to schemaStr1
        + " ]" + "}";

    String schemaStr4 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," // This schema contains all fields.
        + "\"fields\":" + " [" + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"company\",\"type\":\"string\", \"default\" : \"linkedin\"},"
        + "   {\"name\":\"age\",\"type\":\"int\", \"default\" : 29}" + " ]" + "}";

    final SchemaEntry schemaEntry1 = new SchemaEntry(1, schemaStr1);
    final SchemaEntry schemaEntry2 = new SchemaEntry(2, schemaStr2);
    final SchemaEntry schemaEntry3 = new SchemaEntry(3, schemaStr3);
    final SchemaEntry schemaEntry4 = new SchemaEntry(4, schemaStr4);
    final Schema expectedSupersetSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr4);

    // Case 1: generate a superset new schema entry with a new ID.
    SchemaEntry supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
        Arrays.asList(schemaEntry1, schemaEntry2, schemaEntry3));
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
    Assert.assertEquals(supersetSchemaEntry.getId(), 4);

    // Case 2: generate a superset schema entry that is the same as schemaEntry4.
    supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
        Arrays.asList(schemaEntry1, schemaEntry2, schemaEntry4));
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
    Assert.assertEquals(supersetSchemaEntry.getId(), 4);

    // Case 3: any schema + schemaEntry4 can generate a schema entry that is the same as schemaEntry4.
    for (SchemaEntry schemaEntry : Arrays.asList(schemaEntry1, schemaEntry2, schemaEntry3)) {
      supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
          Arrays.asList(schemaEntry, schemaEntry4));
      Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
      Assert.assertEquals(supersetSchemaEntry.getId(), 4);

      supersetSchemaEntry =
          AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(Arrays.asList(schemaEntry4, schemaEntry)
              // Order should not matter.
          );
      Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
      Assert.assertEquals(supersetSchemaEntry.getId(), 4);
    }
  }

  @Test
  public void  testSupersetSchemaDefaultCompatibility() {
    String valueSchemaStr1 = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  " +
        "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  " +
        "       { \"name\": \"weight\", \"type\": \"float\", \"default\": 0},  " +
        "       { \"name\": \"age\", \"type\": \"float\", \"default\": -1 }" +
        "  ] " +
        " } ";

    String valueSchemaStr2 = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  " +
        "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  " +
        "       { \"name\": \"address\", \"type\": \"string\", \"default\": \"italy\" }" +
        "  ] " +
        " } ";

    Schema newValueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr1);
    Schema existingValueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr2);
    Schema newSuperSetSchema = AvroSupersetSchemaUtils.generateSuperSetSchema(existingValueSchema, newValueSchema);
    Assert.assertTrue(new SchemaEntry(1, valueSchemaStr2).isNewSchemaCompatible(new SchemaEntry(2, newSuperSetSchema), DirectionalSchemaCompatibilityType.FULL));
  }

  @Test
  public void testSchemaArrayDocUpdateNestedFieldArray() {
    String str1 =
        "{\"type\":\"record\",\"name\":\"memberFeaturesObject\",\"namespace\":\"com.linkedin.venice.schemas\",\"fields\":[{\"name\":\"visitProbabilityForInvite\",\"type\":\"float\",\"doc\":\"The probability of a member visiting the site after sending this member an invitation. The range of this value will between 0 to 1. The higher the value, the more likely that member will visit the site. \",\"default\":-1},{\"name\":\"activityLevel\",\"type\":\"int\",\"doc\":\"Member activity level, from 0-4. 0-4 means: 0(Onboarding), 1(Dormant), 2(OneByOne), 3(OneByThree), 4(FourbyFour)\",\"default\":0},{\"name\":\"connectionCount\",\"type\":\"int\",\"doc\":\"The total connection count of a member.\",\"default\":0},{\"name\":\"lastConnectionAcceptedAt\",\"type\":\"int\",\"doc\":\"The Unix timestamp (seconds) of the most recently accepted connection.\",\"default\":0},{\"name\":\"pVisitModel\",\"type\":\"string\",\"doc\":\"Different lix treatment(pvisit models) for computing pvisit score.\",\"default\":\"\"},{\"name\":\"pVisitBasicModelScore\",\"type\":\"float\",\"doc\":\"The score is computed based on the pVisit basic model which is simply member visit probability.\",\"default\":0},{\"name\":\"pVisitEqmGainModelScore\",\"type\":\"float\",\"doc\":\"The pvisit score is computed based on a model that uses potential eQM gain as weight during model training. eQM_gain model slightly prefer member visit from who has more eQM gain.\",\"default\":0},{\"name\":\"contribLevel\",\"type\":\"int\",\"doc\":\"Whether the user contribute in the last k days (k=7).\",\"default\":0},{\"name\":\"invReceivedCount\",\"type\":\"int\",\"doc\":\"The number of member connection invitations received by the member in the past 7 days.\",\"default\":0},{\"name\":\"publicContributor\",\"type\":\"int\",\"doc\":\"The member has like/commented/shared or created content that has viral action\",\"default\":0},{\"name\":\"privateContributor\",\"type\":\"int\",\"doc\":\"The member has contributed through private messaging or inMail\",\"default\":0},{\"name\":\"totalActiveConnections\",\"type\":\"int\",\"doc\":\"The number of connections of a member who have made at least 1 contribution (like/comment/share/message) in the past 7 days\",\"default\":0},{\"name\":\"privateActiveConnections\",\"type\":\"int\",\"doc\":\"The number of connections of a member who have made at least 1 private contribution (message/inMail) in the past 7 days\",\"default\":0},{\"name\":\"publicActiveConnections\",\"type\":\"int\",\"doc\":\"The number of connections of a member who have made at least 1 public contribution (like/comment/share/publish) in the past 7 days\",\"default\":0},{\"name\":\"allContributionCount\",\"type\":\"int\",\"doc\":\"The total number of contributions (likes, comments, shares, private messages/inmails) that the member contributed in the past 7 days\",\"default\":0},{\"name\":\"likeCount\",\"type\":\"int\",\"doc\":\"The number of likes that the member contributed in the past 7 days\",\"default\":0},{\"name\":\"commentCount\",\"type\":\"int\",\"doc\":\"The number of comments that the member contributed in the past 7 days\",\"default\":0},{\"name\":\"shareCount\",\"type\":\"int\",\"doc\":\"The number of shares that the member contributed in the past 7 days\",\"default\":0},{\"name\":\"articleContribCount\",\"type\":\"int\",\"doc\":\"The total number of articles that a member has contributed\",\"default\":0},{\"name\":\"privateContribCount\",\"type\":\"int\",\"doc\":\"The number of private messages or inMails that the member has sent in the past 7 days\",\"default\":0},{\"name\":\"allFeedContribCount\",\"type\":\"int\",\"doc\":\"The total number of contributions(likes, comments, shares) in Feed that the member contributed in the past 7 days\",\"default\":0},{\"name\":\"feedLikeCount\",\"type\":\"int\",\"doc\":\"The total number of likes that the member contributed in Feed in the past 7 days\",\"default\":0},{\"name\":\"feedCommentCount\",\"type\":\"int\",\"doc\":\"The total number of comments that the member contributed in Feed in the past 7 days\",\"default\":0},{\"name\":\"feedShareCount\",\"type\":\"int\",\"doc\":\"The total number of shares that the member contributed in Feed in the past 7 days\",\"default\":0},{\"name\":\"evDestLix\",\"type\":\"int\",\"doc\":\"The member is a targeted dest candidate of edge value model\",\"default\":0},{\"name\":\"mfInviter\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The inviter member embedding computed by matrix factorization\",\"default\":[]},{\"name\":\"mfInvitee\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The invitee member embedding computed by matrix factorization\",\"default\":[]},{\"name\":\"boostFactorConsumerOptimus\",\"type\":\"float\",\"doc\":\"Boost factor to be applied to all destinations -- optimus consumer-side experiment\",\"default\":0},{\"name\":\"utilityConsumerOptimus\",\"type\":\"float\",\"doc\":\"Utility score for being destination -- optimus consumer-side experiment\",\"default\":0},{\"name\":\"skillsEmbedding\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The member skills embedding\",\"default\":[]},{\"name\":\"titlesEmbedding\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The member titles embedding\",\"default\":[]},{\"name\":\"companiesEmbedding\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The member companies embedding\",\"default\":[]},{\"name\":\"schoolsEmbedding\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The member schools embedding\",\"default\":[]},{\"name\":\"needleEmbedding\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The member needle embedding\",\"default\":[]},{\"name\":\"profileCompanies\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"profileCompanies\",\"fields\":[{\"name\":\"companyId\",\"type\":\"int\",\"doc\":\"\",\"default\":0},{\"name\":\"industryId\",\"type\":\"int\",\"doc\":\"\",\"default\":0}]}},\"doc\":\"Profile company info\",\"default\":[]},{\"name\":\"profileSchools\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"profileSchools\",\"fields\":[{\"name\":\"schoolId\",\"type\":\"int\",\"doc\":\"\",\"default\":0},{\"name\":\"cipCodeId\",\"type\":\"string\",\"doc\":\"\",\"default\":\"\"},{\"name\":\"degreeId\",\"type\":\"int\",\"doc\":\"\",\"default\":0},{\"name\":\"startTime\",\"type\":\"long\",\"doc\":\"\",\"default\":-1}]}},\"doc\":\"Profile school info\",\"default\":[]},{\"name\":\"profileGeo\",\"type\":\"string\",\"doc\":\"Profile geo\",\"default\":\"\"},{\"name\":\"memberLanguages\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"Inferred languages over a threshold for a member.\",\"default\":[]},{\"name\":\"trianglePcreate\",\"type\":\"float\",\"doc\":\"Member feedback sensitivity (for creation)\",\"default\":0},{\"name\":\"deltaPcreate\",\"type\":\"float\",\"doc\":\"Member feedback delta sensitivity (for creation)\",\"default\":0}]}";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(str1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(str1);
    Assert.assertEquals(s2, s1);
    Schema schema = AvroSupersetSchemaUtils.generateSuperSetSchema(s1,s2);
    Assert.assertEquals(schema, s1);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Assert.assertFalse(AvroSchemaUtils.hasDocFieldChange(s1, s2));
  }

  @Test
  public void testStringVsAvroString() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\": {\"type\" : \"string\", \"avro.java.string\" : \"String\"},\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);

    Assert.assertNotEquals(s1, s2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSuperSetSchema(s2, s1);
    Assert.assertNotNull(s3);
    Assert.assertNotNull(
        AvroCompatibilityHelper.getSchemaPropAsJsonString(s3.getField("name").schema(), "avro.java.string"));
  }

  @Test
  public void testWithDifferentDocField() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(s3);
  }

  @Test
  public void testSchemaMerge() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"business\",\"type\":\"string\"}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));
    Schema s3 = AvroSupersetSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(s3);
  }

  @Test
  public void testSchemaMergeFields() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id1\",\"type\":\"double\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id2\",\"type\":\"int\"}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(s3.getField("id1"));
    Assert.assertNotNull(s3.getField("id2"));
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testWithIncompatibleSchema() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"int\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);

    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));
    AvroSupersetSchemaUtils.generateSuperSetSchema(s1, s2);
  }

  @Test
  public void testSchemaMergeUnion() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : 32},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"string\", \"int\", \"null\"], \"default\" : \"dflt\"},{\"name\":\"organization\",\"type\":\"string\"}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(s3.getField("company"));
    Assert.assertNotNull(s3.getField("organization"));
  }

  @Test
  public void testWithNewFieldArrayRecord() {
    String recordSchemaStr1 = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"namespace\" : \"com.linkedin.avro\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"hits\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"JobAlertHit\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"memberId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"searchId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        } ]\n"
        + "      }\n" +
        "    },\n" +
        "    \"default\" : [ ]\n" +
        "  }, {\n" +
        "    \"name\" : \"hasNext\",\n" +
        "    \"type\" : \"boolean\",\n" +
        "    \"default\" : false\n" +
        "  } ]\n" +
        "}";

    String recordSchemaStr2 = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"namespace\" : \"com.linkedin.avro\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"hits\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"JobAlertHit\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"memberId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"companyId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"searchId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        } ]\n"
        + "      }\n" +
        "    },\n" +
        "    \"default\" : [ ]\n" +
        "  }, {\n" +
        "    \"name\" : \"hasNext\",\n" +
        "    \"type\" : \"boolean\",\n" +
        "    \"default\" : false\n" +
        "  } ]\n" +
        "}";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(recordSchemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(recordSchemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(s3);
  }


  @Test
  public void tesMergeWithDefaultValueUpdate() {
    String schemaStr1 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n" +
        "                }\n" +
        "              },\n" +
        "               {\"name\": \"salary\", \"type\": \"long\", \"default\" : 123}\n" +
        "           ]\n" +
        "        }";
    String schemaStr2 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n" +
        "                } \n" +
        "              },\n" +
        "               {\"name\": \"salary\", \"type\": \"long\", \"default\": 123}" +
        "           ]\n" +
        "        }";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(AvroSchemaUtils.getFieldDefault(s3.getField("salary")));
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testWithEnumEvolution() {
    String schemaStr1 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n" +
        "                } \n" +
        "              }\n" +
        "           ]\n" +
        "        }";
    String schemaStr2 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"CLUBS\"]\n" +
        "                } \n" +
        "              }\n" +
        "           ]\n" +
        "        }";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);

    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));
    AvroSupersetSchemaUtils.generateSuperSetSchema(s1, s2);
  }

  @Test
  public void testIsSupersetSchemaSimpleRecordSchema() {
    String valueSchemaStr1 = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"}  " +
        "  ] " +
        " } ";

    String valueSchemaStr2 = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  " +
        "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"}  " +
        "  ] " +
        " } ";

    String valueSchemaStr3 = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"address\", \"type\": \"string\", \"default\": \"italy\"},  " +
        "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"}  " +
        "  ] " +
        " } ";

    String valueSchemaStr4 = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"address\", \"type\": \"string\", \"default\": \"italy\"},  " +
        "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  " +
        "       { \"name\": \"weight\", \"type\": \"float\", \"default\": 0}  " +
        "  ] " +
        " } ";

    Schema schema1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr1);
    Schema schema2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr2);
    Schema schema3 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr3);
    Schema schema4 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr4);

    // Case 1: a schema must be its own superset schema.
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema1, schema1));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema2, schema2));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema3, schema3));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema4, schema4));

    // Case 2: check is-superset-schema relation between different schemas.
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema2, schema1));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema1, schema2));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema3, schema2));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema3, schema1));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema1, schema3));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema2, schema3));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema3, schema4));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema4, schema3));
  }

  @Test
  public void testIsSupersetSchemaNestedRecordSchema() {
    // Both schemas have a field that is a Record. The inner Record field in the second schema has one more field than
    // that in the first schema. The second schema should be the first schema's superset schema.

    String valueSchemaStr1 = "{" + "    "
        + "\"name\": \"person\"," +
        "  \"type\": \"record\"," +
        "  \"fields\": [" +
        "     {\"name\": \"firstname\", \"type\": \"string\", \"default\": \"john\"}," +
        "     {\"name\": \"lastname\", \"type\": \"string\",  \"default\": \"doe\"}," +
        "     {" +
        "     \"name\": \"address\"," +
        "        \"type\": {" +
        "                   \"type\" : \"record\"," +
        "                   \"name\" : \"AddressUSRecord\"," +
        "                   \"fields\" : [" +
        "                                  {\"name\": \"streetaddress\", \"type\": \"string\",  \"default\": \"101 XX\"}," +
        "                                  {\"name\": \"city\", \"type\": \"string\",  \"default\": \"Sunnyvale\"}" +
        "                                ]" +
        "                  }" +
        "     }" +
        "   ]"
        + "}";

    String valueSchemaStr2 = "{" + "    "
        + "\"name\": \"person\"," +
        "  \"type\": \"record\"," +
        "  \"fields\": [" +
        "     {\"name\": \"firstname\", \"type\": \"string\", \"default\": \"john\"}," +
        "     {\"name\": \"lastname\", \"type\": \"string\",  \"default\": \"doe\"}," +
        "     {" +
        "     \"name\": \"address\"," +
        "        \"type\": {" +
        "                   \"type\" : \"record\"," +
        "                   \"name\" : \"AddressUSRecord\"," +
        "                   \"fields\" : [" +
        "                                  {\"name\": \"streetaddress\", \"type\": \"string\",  \"default\": \"101 XX\"}," +
        "                                  {\"name\": \"city\", \"type\": \"string\",  \"default\": \"Sunnyvale\"}," +
        "                                  {\"name\": \"country\", \"type\": \"string\",  \"default\": \"U.S.A\"}" +
        "                                ]" +
        "                  }" +
        "     }" +
        "   ]"
        + "}";

    Schema schema1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr1);
    Schema schema2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr2);

    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema1, schema1));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema2, schema2));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema2, schema1));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema1, schema2));
  }
}
