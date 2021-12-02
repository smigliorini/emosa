package it.univr.auditel;

import it.univr.auditel.entities.Group;
import it.univr.auditel.entities.GroupView;
import it.univr.auditel.entities.Utils;
import it.univr.auditel.entities.ViewRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class RebuildViewSequences {

  // === Properties ============================================================

  private static final String filePath =
      "C:\\workspace\\projects\\veronacard_analysis\\data\\auditel\\"
          + "log2_3min_groups_clean_timeslot_wdwe.csv";

  private static final String inputSeparator = ";";
  private static final String outputSeparator = ",";

  // minutes between two views to be in the same sequence => 30 minutes
  private static long delta = 30 * 60 * 1000;

  private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  // === Methods ===============================================================

  public static void main(String[] args) throws ParseException, FileNotFoundException {
    final List<String> lines = readLines(new File(filePath), true);
    final HashMap<Integer, GroupView> gvMap = new HashMap<>();

    for (String l : lines) {
      final ViewRecord vr = processLine(l);
      final Integer key = Integer.parseInt(vr.getGroupId());

      if (gvMap.get(key) == null) {
        final Group g = new Group();
        g.setGroupId(Integer.parseInt(vr.getGroupId()));
        g.setFamilyId(vr.getFamilyId());
        g.addUser(vr.getUserId());
        final GroupView gv = new GroupView();
        gv.setEpgChannelId(vr.getEpgChannelId());
        gv.setProgramId(vr.getProgramId());
        gv.setIntervalStart(vr.getStartTime());
        gv.setIntervalEnd(vr.getEndTime());
        gv.setGroup(g);

        gvMap.put(key, gv);

      } else {
        final GroupView current = gvMap.get(key);
        current.addGroupUser(vr.getUserId());
        current.setIntervalStart
            (current.getIntervalStart().compareTo(vr.getStartTime()) < 0 ?
                current.getIntervalStart() :
                vr.getStartTime());
        current.setIntervalEnd
            (current.getIntervalEnd().compareTo(vr.getEndTime()) < 0 ?
                current.getIntervalEnd() :
                vr.getEndTime());
      }
    }

    final HashMap<Integer, GroupView> gvSelMap = new HashMap<>();
    for (Integer k : gvMap.keySet()) {
      final GroupView gv = gvMap.get(k);
      if (gv.getGroup().getUsers().size() > 1) {
        gvSelMap.put(k, gv);
      }
    }

    System.out.printf("Number of group views: %s.%n", gvSelMap.size());

    final Map<Set<String>, List<GroupView>> gvSeqMap = new HashMap<>();
    for (Map.Entry<Integer, GroupView> e : gvSelMap.entrySet()) {
      final Set<String> key = e.getValue().getGroup().getUsers();
      List<GroupView> list = gvSeqMap.get(key);

      if (list == null) {
        list = new ArrayList<>();
      }
      list.add(e.getValue());
      gvSeqMap.put(key, list);
    }

    final Map<Set<String>, List<GroupView>> gvSeqSelMap = new HashMap<>();
    for (Map.Entry<Set<String>, List<GroupView>> e : gvSeqMap.entrySet()) {
      Collections.sort(e.getValue(), new Comparator<GroupView>() {
        @Override
        public int compare(GroupView groupView, GroupView t1) {
          return groupView.getIntervalStart().compareTo(t1.getIntervalStart());
        }
      });
      if (e.getValue().size() > 1) {
        gvSeqSelMap.put(e.getKey(), e.getValue());
      }
    }
    System.out.printf("Number of potential group view sequences: %s.%n", gvSeqSelMap.size());


    final List<List<GroupView>> result = new ArrayList<>();
    Long maxDuration = Long.MIN_VALUE;
    Long minDuration = Long.MAX_VALUE;

    for (Map.Entry<Set<String>, List<GroupView>> e : gvSeqSelMap.entrySet()) {
      final List<Integer> splitIndexes = new ArrayList<>();
      splitIndexes.add(0);

      for (int i = 0; i < e.getValue().size() - 1; i++) {
        final Long x = e.getValue().get(i).getIntervalEnd().getTime();
        final Long y = e.getValue().get(i + 1).getIntervalStart().getTime();

        if (y < x) {
          splitIndexes.add(i + 1);
        } else if ((y - x) > delta) {
          splitIndexes.add(i + 1);
        }
      }

      splitIndexes.add(e.getValue().size());

      for (int s = 0; s < splitIndexes.size() - 1; s++) {
        final List<GroupView> sequence = e.getValue().subList(splitIndexes.get(s), splitIndexes.get(s + 1));
        final long duration = sequence.get(sequence.size() - 1).getIntervalEnd().getTime() -
            sequence.get(0).getIntervalStart().getTime();
        minDuration = Math.min( minDuration, duration );
        maxDuration = Math.max( maxDuration, duration );
        result.add(sequence);
      }
    }
    System.out.printf("Number of historical sequences: %s.%n", result.size());
    System.out.printf( "Min sequence duration: %s min. Max sequece duration: %s min.%n",
        minDuration / 60 / 1000, maxDuration / 60 / 1000 );

    Long minOffset = Long.MAX_VALUE;
    Long maxOffset = Long.MIN_VALUE;
    for (List<GroupView> s : result) {
      for( int k = 0; k < s.size() - 1; k++ ){
        final Long offset = s.get( k+1 ).getIntervalStart().getTime() - s.get( k ).getIntervalEnd().getTime();
        minOffset = Math.min( minOffset, offset );
        maxOffset = Math.max( maxOffset, offset );
      }
    }
    System.out.printf( "Min offset in sequence: %s min. Max offset in sequece: %s min.%n",
        minOffset / 60 / 1000, maxOffset / 60 / 1000 );

    final Map<String, String> ages = readUserAges();

    final StringBuilder b = new StringBuilder();
    for (List<GroupView> s : result) {
      final GroupView g0 = s.get(0);
      // users
      final Iterator<String> it = g0.getGroup().getUsers().iterator();
      while (it.hasNext()) {
        final String u = it.next();
        b.append(u);
        if (it.hasNext()) {
          b.append("-");
        }
      }
      b.append(outputSeparator);

      // family id
      b.append(g0.getGroup().getFamilyId());
      b.append(outputSeparator);

      // group id
      b.append(g0.getGroup().getGroupId());
      b.append(outputSeparator);

      // type set!!!
      Utils.determineGroupType(ages, g0.getGroup());
      final Iterator<String> tit = g0.getGroup().getTypeSet().iterator();
      while (tit.hasNext()) {
        final String type = tit.next();
        b.append(type);
        if (tit.hasNext()) {
          b.append("-");
        }
      }
      b.append(outputSeparator);

      // view sequence
      final Iterator<GroupView> vit = s.iterator();
      while (vit.hasNext()) {
        final GroupView v = vit.next();
        b.append(v.getEpgChannelId());
        b.append(outputSeparator);
        b.append(v.getProgramId());
        b.append(outputSeparator);
        b.append(df.format(v.getIntervalStart()));
        b.append(outputSeparator);
        b.append(df.format(v.getIntervalEnd()));
        b.append(outputSeparator);
        Utils.determineViewTimeSlot(v);
        b.append(v.getTimeSlot());
        if (vit.hasNext()) {
          b.append(outputSeparator);
        }
      }

      b.append(format("%n"));
    }

    writeFile("C:\\workspace\\projects\\veronacard_analysis\\data\\auditel\\",
        "sequence_history.csv", b.toString());
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   * <p>
   * id;user;family_id;group_id;program_id;epg_channel_id;starttime;endtime;time_slot;day_of_week
   *
   * @param line
   */

  private static ViewRecord processLine(String line) throws ParseException {
    if (line == null) {
      throw new NullPointerException();
    }

    final StringTokenizer tk = new StringTokenizer(line, inputSeparator);

    final ViewRecord vr = new ViewRecord();
    int i = 0;

    while (tk.hasMoreTokens()) {
      final String current = tk.nextToken();

      switch (i) {
        case 0:
          // id
          i++;
          break;
        case 1:
          // user
          vr.setUserId(current);
          i++;
          break;
        case 2:
          // family_id
          vr.setFamilyId(current);
          i++;
          break;
        case 3:
          // group_id
          vr.setGroupId(current);
          i++;
          break;
        case 4:
          // program_id
          vr.setProgramId(current);
          i++;
          break;
        case 5:
          // epg_channel_id
          vr.setEpgChannelId(current);
          i++;
          break;
        case 6:
          // starttime
          vr.setStartTime(df.parse(current));
          i++;
          break;
        case 7:
          // endtime
          vr.setEndTime(df.parse(current));
          i++;
          break;
      }
    }

    return vr;
  }

  /**
   * MISSING_COMMENT
   *
   * @param file
   * @param header
   * @return
   */

  private static List<String> readLines(File file, boolean header) {
    final List<String> lines = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      int linevalue = 0;

      while ((line = br.readLine()) != null) {
        if (!header || linevalue > 0) {
          lines.add(line);
        }
        linevalue++;
      }
    } catch (FileNotFoundException e) {
      System.out.printf("File \"%s\" not found.%n", file);
    } catch (IOException e) {
      System.out.printf("Unable to read file \"%s\".%n", file);
    }

    return lines;
  }


  /**
   * MISSING_COMMENT
   *
   * @param dir
   * @param filename
   * @param content
   * @throws FileNotFoundException
   */

  private static void writeFile(String dir, String filename, String content)
      throws FileNotFoundException {

    if (dir == null) {
      throw new NullPointerException();
    }

    if (filename == null) {
      throw new NullPointerException();
    }

    final String path = format("%s\\%s", dir, filename);
    final PrintWriter indexWriter = new PrintWriter(path);
    indexWriter.write(content);
    indexWriter.close();
    System.out.printf("File written in \"%s\".%n", path);
  }


  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public static Map<String, String> readUserAges() {
    final List<String> lines = readLines(new File("C:\\workspace\\projects\\veronacard_analysis\\data\\auditel\\user_demographic_features.csv"), true);
    final Map<String, String> ages = new HashMap<>();
    for (String l : lines) {
      final StringTokenizer tk = new StringTokenizer(l, inputSeparator);
      final String key = tk.nextToken();
      for (int i = 1; i < 6; i++) {
        // "family_id",
        // "is_subscriber",
        // "gender",
        // "year_of_birth",
        // "age"
        tk.nextToken();
      }
      final String value = tk.nextToken();
      ages.put(key, value);
    }

    return ages;
  }


}
