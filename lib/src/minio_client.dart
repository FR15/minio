import 'dart:convert';

import 'package:http/http.dart';
import 'package:minio/minio.dart';
import 'package:minio/src/minio_helpers.dart';
import 'package:minio/src/minio_s3.dart';
import 'package:minio/src/minio_sign.dart';
import 'package:minio/src/utils.dart';


class MinioRequest extends BaseRequest {
  MinioRequest(String method, Uri url) : super(method, url);

  dynamic body;

  @override
  ByteStream finalize() {
    super.finalize();
    if (body is String) {
      final data = utf8.encode(body);
      headers['content-length'] = data.length.toString();
      return ByteStream.fromBytes(utf8.encode(body));
    }
    if (body is List<int>) {
      headers['content-length'] = body.length.toString();
      return ByteStream.fromBytes(body);
    }
    if (body is Stream<List<int>>) {
      return ByteStream(body);
    }
    throw UnsupportedError('unsupported body type: ${body.runtimeType}');
  }

  MinioRequest replace({
    String method,
    Uri url,
    Map<String, String> headers,
    body,
  }) {
    final result = MinioRequest(method ?? this.method, url ?? this.url);
    result.body = body ?? this.body;
    result.headers.addAll(headers ?? this.headers);
    return result;
  }
}

class MinioClient {
  MinioClient(this.minio) {
    anonymous = minio.accessKey.isEmpty && minio.secretKey.isEmpty;
    enableSHA256 = !anonymous && !minio.useSSL;
    port = minio.port ?? implyPort(minio.useSSL);
  }

  final Minio minio;
  final String userAgent = 'MinIO (Unknown; Unknown) minio-dart/0.1.9';

  bool enableSHA256;
  bool anonymous;
  int port;

  String _method;
  String _bucket;
  String _object;

  Future<StreamedResponse> _request({
    String method,
    String bucket,
    String object,
    String region,
    String resource,
    dynamic payload = '',
    Map<String, dynamic> queries,
    Map<String, String> headers,
  }) async {
    region ??= await minio.getBucketRegion(bucket);

    _method = method;
    _bucket = bucket;
    _object = object;

    final request = getBaseRequest(
        method,
        bucket,
        object,
        region,
        resource,
        queries,
        headers);
    request.body = payload;


    final date = DateTime.now().toUtc();
    if (minio.minioWay == MinioWay.OSS) {
      request.headers['date'] = makeDateLong(date);
      _signRequest(request);
    } else {
      final sha256sum = enableSHA256 ? sha256Hex(payload) : 'UNSIGNED-PAYLOAD';
      request.headers.addAll({
        'user-agent': userAgent,
        'x-amz-date': makeDateLong(date),
        'x-amz-content-sha256': sha256sum,
      });

      final authorization = signV4(minio, request, date, region);
      request.headers['authorization'] = authorization;
    }

    logRequest(request);
    final response = await request.send();
    return response;
  }

  void _signRequest(MinioRequest request) {
    if (minio.sessionToken != null) {
      request.headers['x-oss-security-token'] = minio.sessionToken;
    }
    final signature = _makeSignature(request);
    request.headers['authorization'] = "OSS ${minio.accessKey}:$signature";
  }

  String _makeSignature(MinioRequest request) {
    final stringToSign = _getStringToSign(request);
    return hmacSign(minio.secretKey, stringToSign);
  }

  String _getStringToSign(MinioRequest request) {
    final resourceString = _getResourceString(request);
    final headersString = _getHeadersString(request);
    final contentMd5 = request.headers['content-md5'] ?? '';
    final contentType = request.headers['content-type'] ?? '';
    final date = request.headers['date'];

    return "${request.method}\n$contentMd5\n$contentType\n$date\n$headersString$resourceString";
  }

  String _getResourceString(MinioRequest request) {
    if (_bucket == '') {
      return "/";
    } else {
      final substring = '';
      return "/$_bucket/$_object$substring";
    }
  }

  String _getHeadersString(MinioRequest request) {
    var canonHeaders = [];
    for (final key in request.headers.keys) {
      if (key.toLowerCase().startsWith('x-oss-')) {
        canonHeaders.add(key.toLowerCase());
      }
    }
    canonHeaders.sort((s1, s2) {
      return s1.compareTo(s2);
    });
    if (canonHeaders.length > 0) {
      final headerStrings = canonHeaders.map((key) {
        final v = request.headers[key];
        return "$key:$v";
      }).join("\n");
      return "$headerStrings\n";
    } else {
      return '';
    }
  }


  Future<Response> request({
    String method,
    String bucket,
    String object,
    String region,
    String resource,
    dynamic payload = '',
    Map<String, dynamic> queries,
    Map<String, String> headers,
  }) async {
    final stream = await _request(
      method: method,
      bucket: bucket,
      object: object,
      region: region,
      payload: payload,
      resource: resource,
      queries: queries,
      headers: headers,
    );

    final response = await Response.fromStream(stream);
    logResponse(response);

    return response;
  }

  Future<StreamedResponse> requestStream({
    String method,
    String bucket,
    String object,
    String region,
    String resource,
    dynamic payload = '',
    Map<String, dynamic> queries,
    Map<String, String> headers,
  }) async {
    final response = await _request(
      method: method,
      bucket: bucket,
      object: object,
      region: region,
      payload: payload,
      resource: resource,
      queries: queries,
      headers: headers,
    );

    logResponse(response);
    return response;
  }

  MinioRequest getBaseRequest(String method,
      String bucket,
      String object,
      String region,
      String resource,
      Map<String, dynamic> queries,
      Map<String, String> headers,) {
    final url = getRequestUrl(bucket, object, resource, queries);


    //  Uri(
    //   scheme: minio.useSSL ? 'https' : 'http',
    //   host: "$bucket.${minio.endPoint}",
    //   path: "$object",
    // ); //Uri(scheme: "https", host: "$bucket.${minio.endPoint}", path: "$object",pathSegments: path.split('/'),); // "https://$bucket.${minio.endPoint}/$object";;// getRequestUrl(bucket, object, resource, queries);
    final request = MinioRequest(method, url);
    request.headers['host'] = url.authority;

    if (headers != null) {
      request.headers.addAll(headers);
    }

    return request;
  }

  Uri getRequestUrl(String bucket,
      String object,
      String resource,
      Map<String, dynamic> queries,) {
    var host;
    var path;
    if (minio.minioWay == MinioWay.OSS) {
      return Uri(
        scheme: minio.useSSL ? 'https' : 'http',
        host: "$bucket.${minio.endPoint}",
        path: "$object",
      );
    } else {
      host = minio.endPoint.toLowerCase();
      path = '/';
    }


    if (isAmazonEndpoint(host)) {
      host = getS3Endpoint(minio.region);
    }

    if (isVirtualHostStyle(host, minio.useSSL, bucket)) {
      if (bucket != null) host = '${bucket}.${host}';
      if (object != null) path = '/${object}';
    } else {
      if (bucket != null) path = '/${bucket}';
      if (object != null) path = '/${bucket}/${object}';
    }

    final resourcePart = resource == null ? '' : '$resource';
    final queryPart = queries == null ? '' : '&${encodeQueries(queries)}';
    final query = resourcePart + queryPart;

    return Uri(
      scheme: minio.useSSL ? 'https' : 'http',
      host: host,
      port: minio.port,
      pathSegments: path.split('/'),
      query: query,
    );
  }

  void logRequest(MinioRequest request) {
    if (!minio.enableTrace) return;

    final buffer = StringBuffer();
    buffer.writeln('REQUEST: ${request.method} ${request.url}');
    for (var header in request.headers.entries) {
      buffer.writeln('${header.key}: ${header.value}');
    }

    if (request.body is List<int>) {
      buffer.writeln('List<int> of size ${request.body.length}');
    } else {
      buffer.writeln(request.body);
    }

    print(buffer.toString());
  }

  void logResponse(BaseResponse response) {
    if (!minio.enableTrace) return;

    final buffer = StringBuffer();
    buffer.writeln('RESPONSE: ${response.statusCode} ${response.reasonPhrase}');
    for (var header in response.headers.entries) {
      buffer.writeln('${header.key}: ${header.value}');
    }

    if (response is Response) {
      buffer.writeln(response.body);
    } else if (response is StreamedResponse) {
      buffer.writeln('STREAMED BODY');
    }

    print(buffer.toString());
  }
}
