package com.linkedin.alpini.base.hash;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 *
 * FNV1a test vectors from http://www.isthe.com/chongo/src/fnv/test_fnv.c
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class TestFnvHashFunction {

  /* TEST macro does not include trailing NL byte in the test vector */
  private static byte[] TEST(String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
  }

  /* TEST0 macro includes the trailing NL byte in the test vector */
  private static byte[] TEST0(String value) {
    byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
    return Arrays.copyOf(bytes, bytes.length + 1);
  }

  /* REPEAT500 - repeat a string 500 times */
  private static String R500(String value) {
    String r = R100(value);
    return r + r + r + r + r;
  }

  private static byte[] R500(byte[] value) {
    byte[] v = R100(value);
    byte[] r = new byte[v.length * 5];
    int i = 0;
    System.arraycopy(v, 0, r, 0, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    return r;
  }

  private static String R100(String value) {
    String r = R10(value);
    return r + r + r + r + r + r + r + r + r + r;
  }

  private static byte[] R100(byte[] value) {
    byte[] v = R10(value);
    byte[] r = new byte[v.length * 10];
    int i = 0;
    System.arraycopy(v, 0, r, 0, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    return r;
  }

  private static String R10(String r) {
    return r + r + r + r + r + r + r + r + r + r;
  }

  private static byte[] R10(byte[] v) {
    byte[] r = new byte[v.length * 10];
    int i = 0;
    System.arraycopy(v, 0, r, 0, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    System.arraycopy(v, 0, r, i += v.length, v.length);
    return r;
  }

  private static byte[][] fnv_test_str = { TEST(""), TEST("a"), TEST("b"), TEST("c"), TEST("d"), TEST("e"), TEST("f"),
      TEST("fo"), TEST("foo"), TEST("foob"), TEST("fooba"), TEST("foobar"), TEST0(""), TEST0("a"), TEST0("b"),
      TEST0("c"), TEST0("d"), TEST0("e"), TEST0("f"), TEST0("fo"), TEST0("foo"), TEST0("foob"), TEST0("fooba"),
      TEST0("foobar"), TEST("ch"), TEST("cho"), TEST("chon"), TEST("chong"), TEST("chongo"), TEST("chongo "),
      TEST("chongo w"), TEST("chongo wa"), TEST("chongo was"), TEST("chongo was "), TEST("chongo was h"),
      TEST("chongo was he"), TEST("chongo was her"), TEST("chongo was here"), TEST("chongo was here!"),
      TEST("chongo was here!\n"), TEST0("ch"), TEST0("cho"), TEST0("chon"), TEST0("chong"), TEST0("chongo"),
      TEST0("chongo "), TEST0("chongo w"), TEST0("chongo wa"), TEST0("chongo was"), TEST0("chongo was "),
      TEST0("chongo was h"), TEST0("chongo was he"), TEST0("chongo was her"), TEST0("chongo was here"),
      TEST0("chongo was here!"), TEST0("chongo was here!\n"), TEST("cu"), TEST("cur"), TEST("curd"), TEST("curds"),
      TEST("curds "), TEST("curds a"), TEST("curds an"), TEST("curds and"), TEST("curds and "), TEST("curds and w"),
      TEST("curds and wh"), TEST("curds and whe"), TEST("curds and whey"), TEST("curds and whey\n"), TEST0("cu"),
      TEST0("cur"), TEST0("curd"), TEST0("curds"), TEST0("curds "), TEST0("curds a"), TEST0("curds an"),
      TEST0("curds and"), TEST0("curds and "), TEST0("curds and w"), TEST0("curds and wh"), TEST0("curds and whe"),
      TEST0("curds and whey"), TEST0("curds and whey\n"), TEST("hi"), TEST0("hi"), TEST("hello"), TEST0("hello"),
      { (byte) 0xff, 0x00, 0x00, 0x01 }, { 0x01, 0x00, 0x00, (byte) 0xff }, { (byte) 0xff, 0x00, 0x00, 0x02 },
      { 0x02, 0x00, 0x00, (byte) 0xff }, { (byte) 0xff, 0x00, 0x00, 0x03 }, { 0x03, 0x00, 0x00, (byte) 0xff },
      { (byte) 0xff, 0x00, 0x00, 0x04 }, { 0x04, 0x00, 0x00, (byte) 0xff }, { 0x40, 0x51, 0x4e, 0x44 },
      { 0x44, 0x4e, 0x51, 0x40 }, { 0x40, 0x51, 0x4e, 0x4a }, { 0x4a, 0x4e, 0x51, 0x40 }, { 0x40, 0x51, 0x4e, 0x54 },
      { 0x54, 0x4e, 0x51, 0x40 }, TEST("127.0.0.1"), TEST0("127.0.0.1"), TEST("127.0.0.2"), TEST0("127.0.0.2"),
      TEST("127.0.0.3"), TEST0("127.0.0.3"), TEST("64.81.78.68"), TEST0("64.81.78.68"), TEST("64.81.78.74"),
      TEST0("64.81.78.74"), TEST("64.81.78.84"), TEST0("64.81.78.84"), TEST("feedface"), TEST0("feedface"),
      TEST("feedfacedaffdeed"), TEST0("feedfacedaffdeed"), TEST("feedfacedeadbeef"), TEST0("feedfacedeadbeef"),
      TEST("line 1\nline 2\nline 3"), TEST("chongo <Landon Curt Noll> /\\../\\"),
      TEST0("chongo <Landon Curt Noll> /\\../\\"), TEST("chongo (Landon Curt Noll) /\\../\\"),
      TEST0("chongo (Landon Curt Noll) /\\../\\"), TEST("http://antwrp.gsfc.nasa.gov/apod/astropix.html"),
      TEST("http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash"), TEST("http://epod.usra.edu/"),
      TEST("http://exoplanet.eu/"), TEST("http://hvo.wr.usgs.gov/cam3/"), TEST("http://hvo.wr.usgs.gov/cams/HMcam/"),
      TEST("http://hvo.wr.usgs.gov/kilauea/update/deformation.html"),
      TEST("http://hvo.wr.usgs.gov/kilauea/update/images.html"),
      TEST("http://hvo.wr.usgs.gov/kilauea/update/maps.html"),
      TEST("http://hvo.wr.usgs.gov/volcanowatch/current_issue.html"), TEST("http://neo.jpl.nasa.gov/risk/"),
      TEST("http://norvig.com/21-days.html"), TEST("http://primes.utm.edu/curios/home.php"),
      TEST("http://slashdot.org/"), TEST("http://tux.wr.usgs.gov/Maps/155.25-19.5.html"),
      TEST("http://volcano.wr.usgs.gov/kilaueastatus.php"), TEST("http://www.avo.alaska.edu/activity/Redoubt.php"),
      TEST("http://www.dilbert.com/fast/"), TEST("http://www.fourmilab.ch/gravitation/orbits/"),
      TEST("http://www.fpoa.net/"), TEST("http://www.ioccc.org/index.html"),
      TEST("http://www.isthe.com/cgi-bin/number.cgi"), TEST("http://www.isthe.com/chongo/bio.html"),
      TEST("http://www.isthe.com/chongo/index.html"), TEST("http://www.isthe.com/chongo/src/calc/lucas-calc"),
      TEST("http://www.isthe.com/chongo/tech/astro/venus2004.html"),
      TEST("http://www.isthe.com/chongo/tech/astro/vita.html"),
      TEST("http://www.isthe.com/chongo/tech/comp/c/expert.html"),
      TEST("http://www.isthe.com/chongo/tech/comp/calc/index.html"),
      TEST("http://www.isthe.com/chongo/tech/comp/fnv/index.html"),
      TEST("http://www.isthe.com/chongo/tech/math/number/howhigh.html"),
      TEST("http://www.isthe.com/chongo/tech/math/number/number.html"),
      TEST("http://www.isthe.com/chongo/tech/math/prime/mersenne.html"),
      TEST("http://www.isthe.com/chongo/tech/math/prime/mersenne.html#largest"),
      TEST("http://www.lavarnd.org/cgi-bin/corpspeak.cgi"), TEST("http://www.lavarnd.org/cgi-bin/haiku.cgi"),
      TEST("http://www.lavarnd.org/cgi-bin/rand-none.cgi"), TEST("http://www.lavarnd.org/cgi-bin/randdist.cgi"),
      TEST("http://www.lavarnd.org/index.html"), TEST("http://www.lavarnd.org/what/nist-test.html"),
      TEST("http://www.macosxhints.com/"), TEST("http://www.mellis.com/"),
      TEST("http://www.nature.nps.gov/air/webcams/parks/havoso2alert/havoalert.cfm"),
      TEST("http://www.nature.nps.gov/air/webcams/parks/havoso2alert/timelines_24.cfm"),
      TEST("http://www.paulnoll.com/"), TEST("http://www.pepysdiary.com/"),
      TEST("http://www.sciencenews.org/index/home/activity/view"), TEST("http://www.skyandtelescope.com/"),
      TEST("http://www.sput.nl/~rob/sirius.html"), TEST("http://www.systemexperts.com/"),
      TEST("http://www.tq-international.com/phpBB3/index.php"), TEST("http://www.travelquesttours.com/index.htm"),
      TEST("http://www.wunderground.com/global/stations/89606.html"), TEST(R10("21701")), TEST(R10("M21701")),
      TEST(R10("2^21701-1")), R10(new byte[] { 0x54, (byte) 0xc5 }), R10(new byte[] { (byte) 0xc5, 0x54 }),
      TEST(R10("23209")), TEST(R10("M23209")), TEST(R10("2^23209-1")), R10(new byte[] { 0x5a, (byte) 0xa9 }),
      R10(new byte[] { (byte) 0xa9, 0x5a }), TEST(R10("391581216093")), TEST(R10("391581*2^216093-1")),
      R10(new byte[] { 0x05, (byte) 0xf9, (byte) 0x9d, 0x03, 0x4c, (byte) 0x81 }), TEST(R10("FEDCBA9876543210")),
      R10(new byte[] { (byte) 0xfe, (byte) 0xdc, (byte) 0xba, (byte) 0x98, 0x76, 0x54, 0x32, 0x10 }),
      TEST(R10("EFCDAB8967452301")),
      R10(new byte[] { (byte) 0xef, (byte) 0xcd, (byte) 0xab, (byte) 0x89, 0x67, 0x45, 0x23, 0x01 }),
      TEST(R10("0123456789ABCDEF")),
      R10(new byte[] { 0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef }),
      TEST(R10("1032547698BADCFE")),
      R10(new byte[] { 0x10, 0x32, 0x54, 0x76, (byte) 0x98, (byte) 0xba, (byte) 0xdc, (byte) 0xfe }),
      R500(new byte[] { 0x00 }), R500(new byte[] { 0x07 }), TEST(R500("~")), R500(new byte[] { 0x7f }) };

  /* FNV-1a 32 bit test vectors */
  private static Object[][] fnv1a_32_vector = { { fnv_test_str[0], 0x811c9dc5L }, { fnv_test_str[1], 0xe40c292cL },
      { fnv_test_str[2], 0xe70c2de5L }, { fnv_test_str[3], 0xe60c2c52L }, { fnv_test_str[4], 0xe10c2473L },
      { fnv_test_str[5], 0xe00c22e0L }, { fnv_test_str[6], 0xe30c2799L }, { fnv_test_str[7], 0x6222e842L },
      { fnv_test_str[8], 0xa9f37ed7L }, { fnv_test_str[9], 0x3f5076efL }, { fnv_test_str[10], 0x39aaa18aL },
      { fnv_test_str[11], 0xbf9cf968L }, { fnv_test_str[12], 0x050c5d1fL }, { fnv_test_str[13], 0x2b24d044L },
      { fnv_test_str[14], 0x9d2c3f7fL }, { fnv_test_str[15], 0x7729c516L }, { fnv_test_str[16], 0xb91d6109L },
      { fnv_test_str[17], 0x931ae6a0L }, { fnv_test_str[18], 0x052255dbL }, { fnv_test_str[19], 0xbef39fe6L },
      { fnv_test_str[20], 0x6150ac75L }, { fnv_test_str[21], 0x9aab3a3dL }, { fnv_test_str[22], 0x519c4c3eL },
      { fnv_test_str[23], 0x0c1c9eb8L }, { fnv_test_str[24], 0x5f299f4eL }, { fnv_test_str[25], 0xef8580f3L },
      { fnv_test_str[26], 0xac297727L }, { fnv_test_str[27], 0x4546b9c0L }, { fnv_test_str[28], 0xbd564e7dL },
      { fnv_test_str[29], 0x6bdd5c67L }, { fnv_test_str[30], 0xdd77ed30L }, { fnv_test_str[31], 0xf4ca9683L },
      { fnv_test_str[32], 0x4aeb9bd0L }, { fnv_test_str[33], 0xe0e67ad0L }, { fnv_test_str[34], 0xc2d32fa8L },
      { fnv_test_str[35], 0x7f743fb7L }, { fnv_test_str[36], 0x6900631fL }, { fnv_test_str[37], 0xc59c990eL },
      { fnv_test_str[38], 0x448524fdL }, { fnv_test_str[39], 0xd49930d5L }, { fnv_test_str[40], 0x1c85c7caL },
      { fnv_test_str[41], 0x0229fe89L }, { fnv_test_str[42], 0x2c469265L }, { fnv_test_str[43], 0xce566940L },
      { fnv_test_str[44], 0x8bdd8ec7L }, { fnv_test_str[45], 0x34787625L }, { fnv_test_str[46], 0xd3ca6290L },
      { fnv_test_str[47], 0xddeaf039L }, { fnv_test_str[48], 0xc0e64870L }, { fnv_test_str[49], 0xdad35570L },
      { fnv_test_str[50], 0x5a740578L }, { fnv_test_str[51], 0x5b004d15L }, { fnv_test_str[52], 0x6a9c09cdL },
      { fnv_test_str[53], 0x2384f10aL }, { fnv_test_str[54], 0xda993a47L }, { fnv_test_str[55], 0x8227df4fL },
      { fnv_test_str[56], 0x4c298165L }, { fnv_test_str[57], 0xfc563735L }, { fnv_test_str[58], 0x8cb91483L },
      { fnv_test_str[59], 0x775bf5d0L }, { fnv_test_str[60], 0xd5c428d0L }, { fnv_test_str[61], 0x34cc0ea3L },
      { fnv_test_str[62], 0xea3b4cb7L }, { fnv_test_str[63], 0x8e59f029L }, { fnv_test_str[64], 0x2094de2bL },
      { fnv_test_str[65], 0xa65a0ad4L }, { fnv_test_str[66], 0x9bbee5f4L }, { fnv_test_str[67], 0xbe836343L },
      { fnv_test_str[68], 0x22d5344eL }, { fnv_test_str[69], 0x19a1470cL }, { fnv_test_str[70], 0x4a56b1ffL },
      { fnv_test_str[71], 0x70b8e86fL }, { fnv_test_str[72], 0x0a5b4a39L }, { fnv_test_str[73], 0xb5c3f670L },
      { fnv_test_str[74], 0x53cc3f70L }, { fnv_test_str[75], 0xc03b0a99L }, { fnv_test_str[76], 0x7259c415L },
      { fnv_test_str[77], 0x4095108bL }, { fnv_test_str[78], 0x7559bdb1L }, { fnv_test_str[79], 0xb3bf0bbcL },
      { fnv_test_str[80], 0x2183ff1cL }, { fnv_test_str[81], 0x2bd54279L }, { fnv_test_str[82], 0x23a156caL },
      { fnv_test_str[83], 0x64e2d7e4L }, { fnv_test_str[84], 0x683af69aL }, { fnv_test_str[85], 0xaed2346eL },
      { fnv_test_str[86], 0x4f9f2cabL }, { fnv_test_str[87], 0x02935131L }, { fnv_test_str[88], 0xc48fb86dL },
      { fnv_test_str[89], 0x2269f369L }, { fnv_test_str[90], 0xc18fb3b4L }, { fnv_test_str[91], 0x50ef1236L },
      { fnv_test_str[92], 0xc28fb547L }, { fnv_test_str[93], 0x96c3bf47L }, { fnv_test_str[94], 0xbf8fb08eL },
      { fnv_test_str[95], 0xf3e4d49cL }, { fnv_test_str[96], 0x32179058L }, { fnv_test_str[97], 0x280bfee6L },
      { fnv_test_str[98], 0x30178d32L }, { fnv_test_str[99], 0x21addaf8L }, { fnv_test_str[100], 0x4217a988L },
      { fnv_test_str[101], 0x772633d6L }, { fnv_test_str[102], 0x08a3d11eL }, { fnv_test_str[103], 0xb7e2323aL },
      { fnv_test_str[104], 0x07a3cf8bL }, { fnv_test_str[105], 0x91dfb7d1L }, { fnv_test_str[106], 0x06a3cdf8L },
      { fnv_test_str[107], 0x6bdd3d68L }, { fnv_test_str[108], 0x1d5636a7L }, { fnv_test_str[109], 0xd5b808e5L },
      { fnv_test_str[110], 0x1353e852L }, { fnv_test_str[111], 0xbf16b916L }, { fnv_test_str[112], 0xa55b89edL },
      { fnv_test_str[113], 0x3c1a2017L }, { fnv_test_str[114], 0x0588b13cL }, { fnv_test_str[115], 0xf22f0174L },
      { fnv_test_str[116], 0xe83641e1L }, { fnv_test_str[117], 0x6e69b533L }, { fnv_test_str[118], 0xf1760448L },
      { fnv_test_str[119], 0x64c8bd58L }, { fnv_test_str[120], 0x97b4ea23L }, { fnv_test_str[121], 0x9a4e92e6L },
      { fnv_test_str[122], 0xcfb14012L }, { fnv_test_str[123], 0xf01b2511L }, { fnv_test_str[124], 0x0bbb59c3L },
      { fnv_test_str[125], 0xce524afaL }, { fnv_test_str[126], 0xdd16ef45L }, { fnv_test_str[127], 0x60648bb3L },
      { fnv_test_str[128], 0x7fa4bcfcL }, { fnv_test_str[129], 0x5053ae17L }, { fnv_test_str[130], 0xc9302890L },
      { fnv_test_str[131], 0x956ded32L }, { fnv_test_str[132], 0x9136db84L }, { fnv_test_str[133], 0xdf9d3323L },
      { fnv_test_str[134], 0x32bb6cd0L }, { fnv_test_str[135], 0xc8f8385bL }, { fnv_test_str[136], 0xeb08bfbaL },
      { fnv_test_str[137], 0x62cc8e3dL }, { fnv_test_str[138], 0xc3e20f5cL }, { fnv_test_str[139], 0x39e97f17L },
      { fnv_test_str[140], 0x7837b203L }, { fnv_test_str[141], 0x319e877bL }, { fnv_test_str[142], 0xd3e63f89L },
      { fnv_test_str[143], 0x29b50b38L }, { fnv_test_str[144], 0x5ed678b8L }, { fnv_test_str[145], 0xb0d5b793L },
      { fnv_test_str[146], 0x52450be5L }, { fnv_test_str[147], 0xfa72d767L }, { fnv_test_str[148], 0x95066709L },
      { fnv_test_str[149], 0x7f52e123L }, { fnv_test_str[150], 0x76966481L }, { fnv_test_str[151], 0x063258b0L },
      { fnv_test_str[152], 0x2ded6e8aL }, { fnv_test_str[153], 0xb07d7c52L }, { fnv_test_str[154], 0xd0c71b71L },
      { fnv_test_str[155], 0xf684f1bdL }, { fnv_test_str[156], 0x868ecfa8L }, { fnv_test_str[157], 0xf794f684L },
      { fnv_test_str[158], 0xd19701c3L }, { fnv_test_str[159], 0x346e171eL }, { fnv_test_str[160], 0x91f8f676L },
      { fnv_test_str[161], 0x0bf58848L }, { fnv_test_str[162], 0x6317b6d1L }, { fnv_test_str[163], 0xafad4c54L },
      { fnv_test_str[164], 0x0f25681eL }, { fnv_test_str[165], 0x91b18d49L }, { fnv_test_str[166], 0x7d61c12eL },
      { fnv_test_str[167], 0x5147d25cL }, { fnv_test_str[168], 0x9a8b6805L }, { fnv_test_str[169], 0x4cd2a447L },
      { fnv_test_str[170], 0x1e549b14L }, { fnv_test_str[171], 0x2fe1b574L }, { fnv_test_str[172], 0xcf0cd31eL },
      { fnv_test_str[173], 0x6c471669L }, { fnv_test_str[174], 0x0e5eef1eL }, { fnv_test_str[175], 0x2bed3602L },
      { fnv_test_str[176], 0xb26249e0L }, { fnv_test_str[177], 0x2c9b86a4L }, { fnv_test_str[178], 0xe415e2bbL },
      { fnv_test_str[179], 0x18a98d1dL }, { fnv_test_str[180], 0xb7df8b7bL }, { fnv_test_str[181], 0x241e9075L },
      { fnv_test_str[182], 0x063f70ddL }, { fnv_test_str[183], 0x0295aed9L }, { fnv_test_str[184], 0x56a7f781L },
      { fnv_test_str[185], 0x253bc645L }, { fnv_test_str[186], 0x46610921L }, { fnv_test_str[187], 0x7c1577f9L },
      { fnv_test_str[188], 0x512b2851L }, { fnv_test_str[189], 0x76823999L }, { fnv_test_str[190], 0xc0586935L },
      { fnv_test_str[191], 0xf3415c85L }, { fnv_test_str[192], 0x0ae4ff65L }, { fnv_test_str[193], 0x58b79725L },
      { fnv_test_str[194], 0xdea43aa5L }, { fnv_test_str[195], 0x2bb3be35L }, { fnv_test_str[196], 0xea777a45L },
      { fnv_test_str[197], 0x8f21c305L }, { fnv_test_str[198], 0x5c9d0865L }, { fnv_test_str[199], 0xfa823dd5L },
      { fnv_test_str[200], 0x21a27271L }, { fnv_test_str[201], 0x83c5c6d5L }, { fnv_test_str[202], 0x813b0881L }, };

  static Object[][] fnv1a_64_vector = { { fnv_test_str[0], 0xcbf29ce484222325L },
      { fnv_test_str[1], 0xaf63dc4c8601ec8cL }, { fnv_test_str[2], 0xaf63df4c8601f1a5L },
      { fnv_test_str[3], 0xaf63de4c8601eff2L }, { fnv_test_str[4], 0xaf63d94c8601e773L },
      { fnv_test_str[5], 0xaf63d84c8601e5c0L }, { fnv_test_str[6], 0xaf63db4c8601ead9L },
      { fnv_test_str[7], 0x08985907b541d342L }, { fnv_test_str[8], 0xdcb27518fed9d577L },
      { fnv_test_str[9], 0xdd120e790c2512afL }, { fnv_test_str[10], 0xcac165afa2fef40aL },
      { fnv_test_str[11], 0x85944171f73967e8L }, { fnv_test_str[12], 0xaf63bd4c8601b7dfL },
      { fnv_test_str[13], 0x089be207b544f1e4L }, { fnv_test_str[14], 0x08a61407b54d9b5fL },
      { fnv_test_str[15], 0x08a2ae07b54ab836L }, { fnv_test_str[16], 0x0891b007b53c4869L },
      { fnv_test_str[17], 0x088e4a07b5396540L }, { fnv_test_str[18], 0x08987c07b5420ebbL },
      { fnv_test_str[19], 0xdcb28a18fed9f926L }, { fnv_test_str[20], 0xdd1270790c25b935L },
      { fnv_test_str[21], 0xcac146afa2febf5dL }, { fnv_test_str[22], 0x8593d371f738acfeL },
      { fnv_test_str[23], 0x34531ca7168b8f38L }, { fnv_test_str[24], 0x08a25607b54a22aeL },
      { fnv_test_str[25], 0xf5faf0190cf90df3L }, { fnv_test_str[26], 0xf27397910b3221c7L },
      { fnv_test_str[27], 0x2c8c2b76062f22e0L }, { fnv_test_str[28], 0xe150688c8217b8fdL },
      { fnv_test_str[29], 0xf35a83c10e4f1f87L }, { fnv_test_str[30], 0xd1edd10b507344d0L },
      { fnv_test_str[31], 0x2a5ee739b3ddb8c3L }, { fnv_test_str[32], 0xdcfb970ca1c0d310L },
      { fnv_test_str[33], 0x4054da76daa6da90L }, { fnv_test_str[34], 0xf70a2ff589861368L },
      { fnv_test_str[35], 0x4c628b38aed25f17L }, { fnv_test_str[36], 0x9dd1f6510f78189fL },
      { fnv_test_str[37], 0xa3de85bd491270ceL }, { fnv_test_str[38], 0x858e2fa32a55e61dL },
      { fnv_test_str[39], 0x46810940eff5f915L }, { fnv_test_str[40], 0xf5fadd190cf8edaaL },
      { fnv_test_str[41], 0xf273ed910b32b3e9L }, { fnv_test_str[42], 0x2c8c5276062f6525L },
      { fnv_test_str[43], 0xe150b98c821842a0L }, { fnv_test_str[44], 0xf35aa3c10e4f55e7L },
      { fnv_test_str[45], 0xd1ed680b50729265L }, { fnv_test_str[46], 0x2a5f0639b3dded70L },
      { fnv_test_str[47], 0xdcfbaa0ca1c0f359L }, { fnv_test_str[48], 0x4054ba76daa6a430L },
      { fnv_test_str[49], 0xf709c7f5898562b0L }, { fnv_test_str[50], 0x4c62e638aed2f9b8L },
      { fnv_test_str[51], 0x9dd1a8510f779415L }, { fnv_test_str[52], 0xa3de2abd4911d62dL },
      { fnv_test_str[53], 0x858e0ea32a55ae0aL }, { fnv_test_str[54], 0x46810f40eff60347L },
      { fnv_test_str[55], 0xc33bce57bef63eafL }, { fnv_test_str[56], 0x08a24307b54a0265L },
      { fnv_test_str[57], 0xf5b9fd190cc18d15L }, { fnv_test_str[58], 0x4c968290ace35703L },
      { fnv_test_str[59], 0x07174bd5c64d9350L }, { fnv_test_str[60], 0x5a294c3ff5d18750L },
      { fnv_test_str[61], 0x05b3c1aeb308b843L }, { fnv_test_str[62], 0xb92a48da37d0f477L },
      { fnv_test_str[63], 0x73cdddccd80ebc49L }, { fnv_test_str[64], 0xd58c4c13210a266bL },
      { fnv_test_str[65], 0xe78b6081243ec194L }, { fnv_test_str[66], 0xb096f77096a39f34L },
      { fnv_test_str[67], 0xb425c54ff807b6a3L }, { fnv_test_str[68], 0x23e520e2751bb46eL },
      { fnv_test_str[69], 0x1a0b44ccfe1385ecL }, { fnv_test_str[70], 0xf5ba4b190cc2119fL },
      { fnv_test_str[71], 0x4c962690ace2baafL }, { fnv_test_str[72], 0x0716ded5c64cda19L },
      { fnv_test_str[73], 0x5a292c3ff5d150f0L }, { fnv_test_str[74], 0x05b3e0aeb308ecf0L },
      { fnv_test_str[75], 0xb92a5eda37d119d9L }, { fnv_test_str[76], 0x73ce41ccd80f6635L },
      { fnv_test_str[77], 0xd58c2c132109f00bL }, { fnv_test_str[78], 0xe78baf81243f47d1L },
      { fnv_test_str[79], 0xb0968f7096a2ee7cL }, { fnv_test_str[80], 0xb425a84ff807855cL },
      { fnv_test_str[81], 0x23e4e9e2751b56f9L }, { fnv_test_str[82], 0x1a0b4eccfe1396eaL },
      { fnv_test_str[83], 0x54abd453bb2c9004L }, { fnv_test_str[84], 0x08ba5f07b55ec3daL },
      { fnv_test_str[85], 0x337354193006cb6eL }, { fnv_test_str[86], 0xa430d84680aabd0bL },
      { fnv_test_str[87], 0xa9bc8acca21f39b1L }, { fnv_test_str[88], 0x6961196491cc682dL },
      { fnv_test_str[89], 0xad2bb1774799dfe9L }, { fnv_test_str[90], 0x6961166491cc6314L },
      { fnv_test_str[91], 0x8d1bb3904a3b1236L }, { fnv_test_str[92], 0x6961176491cc64c7L },
      { fnv_test_str[93], 0xed205d87f40434c7L }, { fnv_test_str[94], 0x6961146491cc5faeL },
      { fnv_test_str[95], 0xcd3baf5e44f8ad9cL }, { fnv_test_str[96], 0xe3b36596127cd6d8L },
      { fnv_test_str[97], 0xf77f1072c8e8a646L }, { fnv_test_str[98], 0xe3b36396127cd372L },
      { fnv_test_str[99], 0x6067dce9932ad458L }, { fnv_test_str[100], 0xe3b37596127cf208L },
      { fnv_test_str[101], 0x4b7b10fa9fe83936L }, { fnv_test_str[102], 0xaabafe7104d914beL },
      { fnv_test_str[103], 0xf4d3180b3cde3edaL }, { fnv_test_str[104], 0xaabafd7104d9130bL },
      { fnv_test_str[105], 0xf4cfb20b3cdb5bb1L }, { fnv_test_str[106], 0xaabafc7104d91158L },
      { fnv_test_str[107], 0xf4cc4c0b3cd87888L }, { fnv_test_str[108], 0xe729bac5d2a8d3a7L },
      { fnv_test_str[109], 0x74bc0524f4dfa4c5L }, { fnv_test_str[110], 0xe72630c5d2a5b352L },
      { fnv_test_str[111], 0x6b983224ef8fb456L }, { fnv_test_str[112], 0xe73042c5d2ae266dL },
      { fnv_test_str[113], 0x8527e324fdeb4b37L }, { fnv_test_str[114], 0x0a83c86fee952abcL },
      { fnv_test_str[115], 0x7318523267779d74L }, { fnv_test_str[116], 0x3e66d3d56b8caca1L },
      { fnv_test_str[117], 0x956694a5c0095593L }, { fnv_test_str[118], 0xcac54572bb1a6fc8L },
      { fnv_test_str[119], 0xa7a4c9f3edebf0d8L }, { fnv_test_str[120], 0x7829851fac17b143L },
      { fnv_test_str[121], 0x2c8f4c9af81bcf06L }, { fnv_test_str[122], 0xd34e31539740c732L },
      { fnv_test_str[123], 0x3605a2ac253d2db1L }, { fnv_test_str[124], 0x08c11b8346f4a3c3L },
      { fnv_test_str[125], 0x6be396289ce8a6daL }, { fnv_test_str[126], 0xd9b957fb7fe794c5L },
      { fnv_test_str[127], 0x05be33da04560a93L }, { fnv_test_str[128], 0x0957f1577ba9747cL },
      { fnv_test_str[129], 0xda2cc3acc24fba57L }, { fnv_test_str[130], 0x74136f185b29e7f0L },
      { fnv_test_str[131], 0xb2f2b4590edb93b2L }, { fnv_test_str[132], 0xb3608fce8b86ae04L },
      { fnv_test_str[133], 0x4a3a865079359063L }, { fnv_test_str[134], 0x5b3a7ef496880a50L },
      { fnv_test_str[135], 0x48fae3163854c23bL }, { fnv_test_str[136], 0x07aaa640476e0b9aL },
      { fnv_test_str[137], 0x2f653656383a687dL }, { fnv_test_str[138], 0xa1031f8e7599d79cL },
      { fnv_test_str[139], 0xa31908178ff92477L }, { fnv_test_str[140], 0x097edf3c14c3fb83L },
      { fnv_test_str[141], 0xb51ca83feaa0971bL }, { fnv_test_str[142], 0xdd3c0d96d784f2e9L },
      { fnv_test_str[143], 0x86cd26a9ea767d78L }, { fnv_test_str[144], 0xe6b215ff54a30c18L },
      { fnv_test_str[145], 0xec5b06a1c5531093L }, { fnv_test_str[146], 0x45665a929f9ec5e5L },
      { fnv_test_str[147], 0x8c7609b4a9f10907L }, { fnv_test_str[148], 0x89aac3a491f0d729L },
      { fnv_test_str[149], 0x32ce6b26e0f4a403L }, { fnv_test_str[150], 0x614ab44e02b53e01L },
      { fnv_test_str[151], 0xfa6472eb6eef3290L }, { fnv_test_str[152], 0x9e5d75eb1948eb6aL },
      { fnv_test_str[153], 0xb6d12ad4a8671852L }, { fnv_test_str[154], 0x88826f56eba07af1L },
      { fnv_test_str[155], 0x44535bf2645bc0fdL }, { fnv_test_str[156], 0x169388ffc21e3728L },
      { fnv_test_str[157], 0xf68aac9e396d8224L }, { fnv_test_str[158], 0x8e87d7e7472b3883L },
      { fnv_test_str[159], 0x295c26caa8b423deL }, { fnv_test_str[160], 0x322c814292e72176L },
      { fnv_test_str[161], 0x8a06550eb8af7268L }, { fnv_test_str[162], 0xef86d60e661bcf71L },
      { fnv_test_str[163], 0x9e5426c87f30ee54L }, { fnv_test_str[164], 0xf1ea8aa826fd047eL },
      { fnv_test_str[165], 0x0babaf9a642cb769L }, { fnv_test_str[166], 0x4b3341d4068d012eL },
      { fnv_test_str[167], 0xd15605cbc30a335cL }, { fnv_test_str[168], 0x5b21060aed8412e5L },
      { fnv_test_str[169], 0x45e2cda1ce6f4227L }, { fnv_test_str[170], 0x50ae3745033ad7d4L },
      { fnv_test_str[171], 0xaa4588ced46bf414L }, { fnv_test_str[172], 0xc1b0056c4a95467eL },
      { fnv_test_str[173], 0x56576a71de8b4089L }, { fnv_test_str[174], 0xbf20965fa6dc927eL },
      { fnv_test_str[175], 0x569f8383c2040882L }, { fnv_test_str[176], 0xe1e772fba08feca0L },
      { fnv_test_str[177], 0x4ced94af97138ac4L }, { fnv_test_str[178], 0xc4112ffb337a82fbL },
      { fnv_test_str[179], 0xd64a4fd41de38b7dL }, { fnv_test_str[180], 0x4cfc32329edebcbbL },
      { fnv_test_str[181], 0x0803564445050395L }, { fnv_test_str[182], 0xaa1574ecf4642ffdL },
      { fnv_test_str[183], 0x694bc4e54cc315f9L }, { fnv_test_str[184], 0xa3d7cb273b011721L },
      { fnv_test_str[185], 0x577c2f8b6115bfa5L }, { fnv_test_str[186], 0xb7ec8c1a769fb4c1L },
      { fnv_test_str[187], 0x5d5cfce63359ab19L }, { fnv_test_str[188], 0x33b96c3cd65b5f71L },
      { fnv_test_str[189], 0xd845097780602bb9L }, { fnv_test_str[190], 0x84d47645d02da3d5L },
      { fnv_test_str[191], 0x83544f33b58773a5L }, { fnv_test_str[192], 0x9175cbb2160836c5L },
      { fnv_test_str[193], 0xc71b3bc175e72bc5L }, { fnv_test_str[194], 0x636806ac222ec985L },
      { fnv_test_str[195], 0xb6ef0e6950f52ed5L }, { fnv_test_str[196], 0xead3d8a0f3dfdaa5L },
      { fnv_test_str[197], 0x922908fe9a861ba5L }, { fnv_test_str[198], 0x6d4821de275fd5c5L },
      { fnv_test_str[199], 0x1fe3fce62bd816b5L }, { fnv_test_str[200], 0xc23e9fccd6f70591L },
      { fnv_test_str[201], 0xc1af12bdfe16b5b5L }, { fnv_test_str[202], 0x39e9f18f2f85e221L }, };

  FnvHashFunction fnv = new FnvHashFunction();

  @DataProvider(name = "FNV1a32")
  public Object[][] getFnv1_32_vector() {
    return fnv1a_32_vector;
  }

  @DataProvider(name = "FNV1a64")
  public Object[][] getFnv1_64_vector() {
    return fnv1a_64_vector;
  }

  @Test(groups = "unit", dataProvider = "FNV1a32")
  public void testFnvHashFunction32(byte[] test, long hash) {
    // Technically, our FNV1a Hash is bad because we compute 32bit FNV1a
    // but then we do not return only a 32bit value, leaving garbage values
    // in the high word.

    // This means that we cannot validate the bucketing. Boo.

    Assert.assertEquals(0xffffffffL & fnv.hash(test), hash);
    Assert.assertEquals(0xffffffffL & fnv.hash(ByteBuffer.wrap(test)), hash);
  }

  // Disabled because we do not yet have a real 64bit FNV implementation.
  // @Test(groups = "unit", dataProvider = "FNV1a64")
  // public void testFnvHashFunction64(byte[] test, long hash)
  // {
  // Assert.assertEquals(fnv.hash(test), hash);
  // Assert.assertEquals(fnv.hash(ByteBuffer.wrap(test)), hash);
  // }
}
