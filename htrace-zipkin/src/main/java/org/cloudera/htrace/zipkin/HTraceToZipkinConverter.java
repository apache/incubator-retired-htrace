/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudera.htrace.zipkin;

import com.twitter.zipkin.gen.Annotation;
import com.twitter.zipkin.gen.AnnotationType;
import com.twitter.zipkin.gen.BinaryAnnotation;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;
import com.twitter.zipkin.gen.zipkinCoreConstants;
import org.cloudera.htrace.TimelineAnnotation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for converting a HTrace.Span to a Zipkin.Span object. To use the Zipkin
 * infrastructure (collector, front end), we need to store the Span information in a zipkin specific
 * format. This class transforms a HTrace:Span object to a Zipkin:Span object.
 * <p>
 * This is how both Span objects are related:
 * <table>
 * <col width="50%"/> <col width="50%"/> <thead>
 * <tr>
 * <th>HTrace:Span</th>
 * <th>Zipkin:Span</th>
 * </tr>
 * <thead> <tbody>
 * <tr>
 * <td>TraceId</td>
 * <td>TraceId</td>
 * </tr>
 * <tr>
 * <td>ParentId</td>
 * <td>ParentId</td>
 * </tr>
 * <tr>
 * <td>SpanId</td>
 * <td>id</td>
 * </tr>
 * <tr>
 * <td>Description</td>
 * <td>Name</td>
 * </tr>
 * <tr>
 * <td>startTime, stopTime</td>
 * <td>Annotations (cs, cr, sr, ss)</td>
 * </tr>
 * <tr>
 * <td>Other annotations</td>
 * <td>Annotations</td>
 * </tr>
 * </tbody>
 * </table>
 * <p>
 */
public class HTraceToZipkinConverter {

  private final int ipv4Address;
  private final short port;
  private final boolean inClientMode;

  public HTraceToZipkinConverter(int ipv4Address, short port, boolean inClientMode) {
    this.ipv4Address = ipv4Address;
    this.port = port;
    this.inClientMode = inClientMode;
  }

  /**
   * Converts a given HTrace span to a Zipkin Span.
   * <ul>
   * <li>First set the start annotation. [CS, SR], depending whether it is a client service or not.
   * <li>Set other id's, etc [TraceId's etc]
   * <li>Create binary annotations based on data from HTrace Span object.
   * <li>Set the last annotation. [SS, CR]
   * </ul>
   */
  public Span toZipkinSpan(org.cloudera.htrace.Span hTraceSpan) {
    Span zipkinSpan = new Span();
    Endpoint ep = new Endpoint(ipv4Address, port, hTraceSpan.getProcessId());
    List<Annotation> annotationList = createZipkinAnnotations(hTraceSpan, ep);
    List<BinaryAnnotation> binaryAnnotationList = createZipkinBinaryAnnotations(hTraceSpan, ep);
    zipkinSpan.setTrace_id(hTraceSpan.getTraceId());
    if (hTraceSpan.getParentId() != org.cloudera.htrace.Span.ROOT_SPAN_ID) {
      zipkinSpan.setParent_id(hTraceSpan.getParentId());
    }
    zipkinSpan.setId(hTraceSpan.getSpanId());
    zipkinSpan.setName(hTraceSpan.getDescription());
    zipkinSpan.setAnnotations(annotationList);
    zipkinSpan.setBinary_annotations(binaryAnnotationList);
    return zipkinSpan;
  }

  /**
   * Add annotations from the htrace Span.
   */
  private List<Annotation> createZipkinAnnotations(org.cloudera.htrace.Span hTraceSpan,
                                                    Endpoint ep) {
    List<Annotation> annotationList = new ArrayList<Annotation>();

    String firstAnno =
        inClientMode ? zipkinCoreConstants.CLIENT_SEND : zipkinCoreConstants.SERVER_RECV;
    String lastAnno =
        inClientMode ? zipkinCoreConstants.CLIENT_RECV : zipkinCoreConstants.SERVER_SEND;

    // add first zipkin  annotation.
    annotationList.add(createZipkinAnnotation(firstAnno, hTraceSpan, ep, true));
    // add HTrace time annotation
    for (TimelineAnnotation ta : hTraceSpan.getTimelineAnnotations()) {
      annotationList.add(createZipkinAnnotation(ta.getMessage(), hTraceSpan, ep, true));
    }
    // add last zipkin annotation
    annotationList.add(createZipkinAnnotation(lastAnno, hTraceSpan, ep, false));
    return annotationList;
  }

  /**
   * Creates a list of Annotations that are present in HTrace Span object.
   * @return list of Annotations that could be added to Zipkin Span.
   */
  private List<BinaryAnnotation> createZipkinBinaryAnnotations(org.cloudera.htrace.Span span,
                                                               Endpoint ep) {
    List<BinaryAnnotation> l = new ArrayList<BinaryAnnotation>();
    for (Map.Entry<byte[], byte[]> e : span.getKVAnnotations().entrySet()) {
      BinaryAnnotation binaryAnn = new BinaryAnnotation();
      binaryAnn.setAnnotation_type(AnnotationType.BYTES);
      binaryAnn.setKey(new String(e.getKey()));
      binaryAnn.setValue(e.getValue());
      binaryAnn.setHost(ep);
      l.add(binaryAnn);
    }
    return l;
  }

  /**
   * Create an annotation with the correct times and endpoint.
   * @param value Annotation value
   * @param span Span from which timestamp will be extracted
   * @param ep the endopint this annotation will be associated with.
   * @param sendRequest use the first or last timestamp.
   */
  private static Annotation createZipkinAnnotation(String value, org.cloudera.htrace.Span span,
      Endpoint ep, boolean sendRequest) {
    Annotation annotation = new Annotation();
    annotation.setHost(ep);

    // Zipkin is in microseconds
    if (sendRequest) {
      annotation.setTimestamp(span.getStartTimeMillis() * 1000);
    }
    else {
      annotation.setTimestamp(span.getStopTimeMillis() * 1000);
    }

    annotation.setDuration((int) span.getAccumulatedMillis());
    annotation.setValue(value);
    return annotation;
  }
}
