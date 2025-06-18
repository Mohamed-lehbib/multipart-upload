import 'dart:convert';
import 'dart:io';
import 'package:flutter/material.dart';
import 'package:image_picker/image_picker.dart';
import 'package:http/http.dart' as http;
import 'package:dio/dio.dart';
import 'package:path/path.dart' as path;
import 'package:mime/mime.dart';

void main() {
  runApp(const MyApp());
}

//const backendUrl = 'http://192.168.100.24:8000'; // Update if needed
const backendUrl =
    'https://multipart-upload.mlehbib.com'; // Change to your backend IP if needed

class MyApp extends StatelessWidget {
  const MyApp({super.key});
  @override
  Widget build(BuildContext context) {
    return const MaterialApp(home: MultiImageUploader());
  }
}

class MultiImageUploader extends StatefulWidget {
  const MultiImageUploader({super.key});

  @override
  State<MultiImageUploader> createState() => _MultiImageUploaderState();
}

class _MultiImageUploaderState extends State<MultiImageUploader> {
  final ImagePicker _picker = ImagePicker();
  List<File> _selectedImages = [];
  List<String> _uploadStatuses = [];
  bool _isUploading = false;

  Future<void> pickImages() async {
    final pickedFiles = await _picker.pickMultiImage();
    if (pickedFiles.isNotEmpty) {
      setState(() {
        _selectedImages = pickedFiles.map((x) => File(x.path)).toList();
        _uploadStatuses = List.filled(_selectedImages.length, "Not uploaded");
      });
    }
  }

  Future<void> uploadImage(int index) async {
    final file = _selectedImages[index];
    final fileName = path.basename(file.path);
    final contentType = lookupMimeType(file.path) ?? 'application/octet-stream';
    final fileBytes = await file.readAsBytes();
    final fileSize = fileBytes.length;

    setState(() {
      _uploadStatuses[index] = "Starting upload...";
    });

    // Step 1: Initiate multipart upload
    final initiateRes = await http.post(
      Uri.parse('$backendUrl/upload/initiate'),
      body: {'filename': fileName, 'content_type': contentType},
    );

    if (initiateRes.statusCode != 200) {
      setState(() {
        _uploadStatuses[index] =
            'Failed to initiate upload: ${initiateRes.body}';
      });
      return;
    }

    final initJson = json.decode(initiateRes.body);
    final uploadId = initJson['uploadId'];
    final key = initJson['key'];

    const chunkSize = 5 * 1024 * 1024; // 5MB
    final partCount = (fileSize / chunkSize).ceil();
    final etags = <Map<String, String>>[];

    // Step 2: Upload each part
    for (int i = 0; i < partCount; i++) {
      final start = i * chunkSize;
      final end =
          ((i + 1) * chunkSize < fileSize) ? (i + 1) * chunkSize : fileSize;
      final chunk = fileBytes.sublist(start, end);

      final presignRes = await http.post(
        Uri.parse('$backendUrl/upload/presigned-url'),
        body: {
          'key': key,
          'uploadId': uploadId,
          'partNumber': (i + 1).toString(),
        },
      );

      if (presignRes.statusCode != 200) {
        setState(() {
          _uploadStatuses[index] =
              'Failed to get presigned URL: ${presignRes.body}';
        });
        return;
      }

      final presignJson = json.decode(presignRes.body);
      final url = presignJson['url'];

      final dio = Dio();
      dio.interceptors.add(LogInterceptor(
        request: true,
        requestHeader: true,
        requestBody: true,
        responseHeader: true,
        responseBody: true,
        error: true,
      ));
      try {
        // final uploadRes = await dio.put(
        //   url,
        //   data: Stream.fromIterable([chunk]),
        //   options: Options(
        //     headers: {
        //       'Content-Length': chunk.length.toString(),
        //       'Content-Type': 'application/octet-stream',
        //     },
        //   ),
        // );
        final uploadRes = await dio.put(
          url,
          data: chunk, // Use raw bytes instead of Stream
          options: Options(
            headers: {
              'Content-Length': chunk.length.toString(),
              'Content-Type': 'application/octet-stream',
            },
            validateStatus: (status) =>
                status == 200, // Only accept 200 as success
          ),
        );
        final etag = uploadRes.headers.map['etag']?.first;
        if (etag == null) {
          setState(() {
            _uploadStatuses[index] = 'Failed: No ETag returned';
          });
          return;
        }

        etags.add({'PartNumber': '${i + 1}', 'ETag': etag});
        setState(() {
          _uploadStatuses[index] = 'Uploaded part ${i + 1} of $partCount';
        });
      } catch (e, stackTrace) {
        print('Error: $e');
        print('Stack trace: $stackTrace');
        setState(() {
          _uploadStatuses[index] = 'Upload failed: ${e.toString()}';
        });
      }
    }

    // Step 3: Complete upload
    final completeParts =
        etags.map((e) => "${e['PartNumber']}:${e['ETag']}").toList();

    final completeRes = await http.post(
      Uri.parse('$backendUrl/upload/complete'),
      headers: {'Content-Type': 'application/json'},
      body: json.encode({
        // Explicitly encode to JSON
        'key': key,
        'uploadId': uploadId,
        'parts': etags
            .map((e) =>
                {'PartNumber': int.parse(e['PartNumber']!), 'ETag': e['ETag']})
            .toList(),
      }),
    );

    if (completeRes.statusCode == 200) {
      setState(() {
        _uploadStatuses[index] = 'Upload complete!';
      });
    } else {
      setState(() {
        _uploadStatuses[index] =
            'Failed to complete upload: ${completeRes.body}';
      });
    }
  }

  Future<void> uploadAll() async {
    if (_selectedImages.isEmpty) return;

    setState(() {
      _isUploading = true;
    });

    for (int i = 0; i < _selectedImages.length; i++) {
      await uploadImage(i);
    }

    setState(() {
      _isUploading = false;
    });
  }

  @override
  Widget build(BuildContext context) {
    return SafeArea(
      child: Scaffold(
        appBar: AppBar(title: const Text("Multi Image Multipart Upload")),
        body: Padding(
          padding: const EdgeInsets.all(16.0),
          child: Column(
            children: [
              ElevatedButton(
                onPressed: _isUploading ? null : pickImages,
                child: const Text("Pick Images"),
              ),
              const SizedBox(height: 10),
              Expanded(
                child: ListView.builder(
                  itemCount: _selectedImages.length,
                  itemBuilder: (context, index) {
                    print(path.basename(_selectedImages[index].path));
                    print(path.basename(_uploadStatuses[index]));

                    return ListTile(
                      leading: Image.file(
                        _selectedImages[index],
                        width: 50,
                        height: 50,
                        fit: BoxFit.cover,
                      ),
                      title: Text(path.basename(_selectedImages[index].path)),
                      subtitle: Text(
                        _uploadStatuses.length > index
                            ? _uploadStatuses[index]
                            : "",
                      ),
                    );
                  },
                ),
              ),
              ElevatedButton(
                onPressed: (_isUploading || _selectedImages.isEmpty)
                    ? null
                    : uploadAll,
                child:
                    Text(_isUploading ? 'Uploading...' : 'Upload All Images'),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
